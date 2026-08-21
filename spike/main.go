// MinIO 조건부 요청 스파이크
//
// 확인 목적:
//
//	A. If-Match PUT / If-None-Match PUT / 조건부 GET 이 규격대로 412·304 를 반환하는가
//	B. 조건부 PUT 이 실제로 lost update 를 막는가 (워커 여러 개 동시 실행)
//
// B 가 핵심임. A 는 API 가 응답 코드를 준다는 것까지만 보고,
// B 가 "그래서 데이터가 사라지지 않는다" 를 확인함.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

const (
	endpoint  = "http://localhost:9000"
	bucket    = "spike"
	region    = "us-east-1"
	accessKey = "minioadmin"
	secretKey = "minioadmin"

	defaultWorkers = 3  // 동시 실행 워커 수
	defaultIters   = 20 // 워커당 삽입 횟수
)

// 경쟁 수준을 바꿔가며 관측하기 위해 환경변수로 덮어쓸 수 있게 함
var (
	workers = envInt("SPIKE_WORKERS", defaultWorkers)
	iters   = envInt("SPIKE_ITERS", defaultIters)
)

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

var passCount, failCount int

func main() {
	ctx := context.Background()
	c := newClient()

	if err := ensureBucket(ctx, c); err != nil {
		fmt.Println("버킷 준비 실패:", err)
		os.Exit(1)
	}

	fmt.Println("=== A. 조건부 요청 기본 동작 ===")
	basicScenarios(ctx, c)

	fmt.Println("\n=== B. 동시 read-modify-write: 조건부 PUT 없음 ===")
	exp, got, _ := concurrency(ctx, c, "concurrent-off", false)
	fmt.Printf("    기대 %d건 / 실제 %d건 / 유실 %d건\n", exp, got, exp-got)
	check(10, "조건부 없음 → 유실 발생", true, exp-got > 0)

	fmt.Println("\n=== C. 동시 read-modify-write: 조건부 PUT 사용 ===")
	exp, got, conflicts := concurrency(ctx, c, "concurrent-on", true)
	fmt.Printf("    기대 %d건 / 실제 %d건 / 유실 %d건 / 412 충돌 %d회\n", exp, got, exp-got, conflicts)
	check(11, "조건부 사용 → 유실 0", 0, exp-got)

	fmt.Printf("\n=== 결과: 통과 %d / 실패 %d ===\n", passCount, failCount)
	if failCount > 0 {
		os.Exit(1)
	}
}

// basicScenarios 는 조건부 요청의 응답 코드를 시나리오별로 확인함
func basicScenarios(ctx context.Context, c *s3.Client) {
	key := "basic.bin"

	etag1, st, _ := put(ctx, c, key, []byte("v1"), nil, nil)
	check(1, "기본 PUT", 200, st)
	check(2, "ETag 가 strong (W/ 없음)", true, etag1 != "" && !strings.HasPrefix(etag1, "W/"))
	fmt.Printf("    ETag = %s\n", etag1)

	etag2, st, _ := put(ctx, c, key, []byte("v2"), aws.String(etag1), nil)
	check(3, "If-Match 일치 PUT", 200, st)

	_, st, err := put(ctx, c, key, []byte("v3"), aws.String(`"deadbeefdeadbeefdeadbeefdeadbeef"`), nil)
	check(4, "If-Match 불일치 PUT", 412, st)
	fmt.Printf("    에러 코드 = %s\n", errCode(err))

	newKey := "created-once.bin"
	_, _ = c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(newKey)})

	_, st, _ = put(ctx, c, newKey, []byte("x"), nil, aws.String("*"))
	check(5, "If-None-Match:* 신규 생성", 200, st)

	_, st, err = put(ctx, c, newKey, []byte("y"), nil, aws.String("*"))
	check(6, "If-None-Match:* 기존 키", 412, st)
	fmt.Printf("    에러 코드 = %s\n", errCode(err))

	_, _, st, _ = get(ctx, c, key, aws.String(etag2))
	check(7, "조건부 GET 미변경", 304, st)

	if _, _, err := put(ctx, c, key, []byte("v4"), nil, nil); err != nil {
		fmt.Println("    v4 업로드 실패:", err)
	}
	body, _, st, _ := get(ctx, c, key, aws.String(etag2))
	check(8, "조건부 GET 변경됨", 200, st)
	check(9, "변경 후 본문 수신", "v4", string(body))
}

// concurrency 는 워커 여러 개가 같은 객체에 read-modify-write 를 반복하게 하고
// 최종 객체에 몇 건이 남았는지 셈. conditional 이면 If-Match 를 붙이고 412 시 재시도함.
//
// 레코드는 8바이트 빅엔디언 uint64 하나. 최종 크기 / 8 이 살아남은 건수임.
func concurrency(ctx context.Context, c *s3.Client, key string, conditional bool) (expected, actual, conflicts int) {
	if _, _, err := put(ctx, c, key, []byte{}, nil, nil); err != nil {
		fmt.Println("    초기화 실패:", err)
		return 0, 0, 0
	}

	var mu sync.Mutex
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				id := uint64(w*10000 + i)

				// 조건부일 때는 412 를 받으면 다시 읽고 재시도함
				for attempt := 0; attempt < 200; attempt++ {
					body, etag, _, err := get(ctx, c, key, nil)
					if err != nil {
						return
					}

					rec := make([]byte, 8)
					binary.BigEndian.PutUint64(rec, id)
					merged := append(append([]byte{}, body...), rec...)

					var ifMatch *string
					if conditional {
						ifMatch = aws.String(etag)
					}

					_, st, perr := put(ctx, c, key, merged, ifMatch, nil)
					if perr == nil {
						break
					}
					if st == 412 && conditional {
						mu.Lock()
						conflicts++
						mu.Unlock()
						continue
					}
					return
				}
			}
		}(w)
	}
	wg.Wait()

	final, _, _, _ := get(ctx, c, key, nil)
	return workers * iters, len(final) / 8, conflicts
}

func newClient() *s3.Client {
	return s3.New(s3.Options{
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true, // MinIO 는 가상 호스트 방식이 아니라 경로 방식
	})
}

func ensureBucket(ctx context.Context, c *s3.Client) error {
	_, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return nil
	}
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if errors.As(err, &owned) || errors.As(err, &exists) {
		return nil
	}
	return err
}

// put 은 조건부 헤더를 선택적으로 붙여 객체를 올림. (ETag, HTTP 상태, 에러) 반환
func put(ctx context.Context, c *s3.Client, key string, body []byte, ifMatch, ifNoneMatch *string) (string, int, error) {
	out, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		IfMatch:     ifMatch,
		IfNoneMatch: ifNoneMatch,
	})
	if err != nil {
		return "", statusOf(err), err
	}
	return aws.ToString(out.ETag), 200, nil
}

// get 은 조건부 GET 을 수행함. (본문, ETag, HTTP 상태, 에러) 반환
func get(ctx context.Context, c *s3.Client, key string, ifNoneMatch *string) ([]byte, string, int, error) {
	out, err := c.GetObject(ctx, &s3.GetObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		IfNoneMatch: ifNoneMatch,
	})
	if err != nil {
		return nil, "", statusOf(err), err
	}
	defer out.Body.Close()

	b, rerr := io.ReadAll(out.Body)
	if rerr != nil {
		return nil, "", -1, rerr
	}
	return b, aws.ToString(out.ETag), 200, nil
}

// statusOf 는 에러에서 HTTP 상태 코드를 꺼냄.
// AWS SDK 는 304·412 같은 비2xx 응답을 에러로 반환하므로 여기서 코드를 봐야 함
func statusOf(err error) int {
	if err == nil {
		return 200
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return -1
}

// errCode 는 S3 에러 코드 문자열을 꺼냄 (예: PreconditionFailed)
func errCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

func check(id int, desc string, want, got any) {
	ok := fmt.Sprint(want) == fmt.Sprint(got)
	mark := "ok"
	if ok {
		passCount++
	} else {
		mark = "FAIL"
		failCount++
	}
	fmt.Printf("[%2d] %-30s 기대=%-6v 실제=%-6v %s\n", id, desc, want, got, mark)
}
