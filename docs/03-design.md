# 실습 시스템 설계

4단계 산출물. 이 문서만 보고 구현에 착수할 수 있어야 하므로 미결정 사항을 남기지 않음.

- 원문 설계: [01-article-notes.md](01-article-notes.md)
- 개념: [00-prerequisites.md](00-prerequisites.md)
- 조건부 요청 검증 결과: [02-spike-minio.md](02-spike-minio.md)

---

## 1. 목표

Canva 원문의 세션 취소 아키텍처를 같은 방식으로 직접 구현해서 동작시킴.
원문 주장을 검증하는 것이 목적이 아니므로 기존 방식(MySQL 직접 조회) 대조 구현은 만들지 않음.

## 2. 컴포넌트

```
                    ┌──────────┐
   로그인/로그아웃 ──→│   auth   │──┐
                    └──────────┘  │  INSERT revocations
                                  ↓
                            ┌───────────┐
                            │   MySQL   │  원장 (source of truth)
                            └───────────┘
                                  │  SELECT uploaded_at IS NULL
                                  ↓
                    ┌──────────────────────┐
                    │  worker × 3          │
                    │  조건부 PUT + 412 재시도│
                    └──────────────────────┘
                                  │
                                  ↓
                            ┌───────────┐
                            │   MinIO   │  청크 저장소
                            └───────────┘
                                  │  부팅 시딩 + 조건부 GET 폴링
                                  ↓
                    ┌──────────────────────┐
      API 요청 ────→ │  gateway × 3         │──→ 200 / 401
                    │  메모리 이진 탐색      │
                    └──────────────────────┘
                                  │  쿠키 갱신 시에만
                                  └──────────→ MySQL
```

| 컴포넌트 | 개수 | 역할 |
|---|---|---|
| auth | 1 | 로그인 시 쿠키 발급, 로그아웃 시 MySQL 에 취소 기록 |
| worker | 3 | MySQL → MinIO 청크 반영. 3개는 조건부 PUT 경쟁을 실제로 일으키기 위함 |
| gateway | 3 | 요청마다 쿠키 검증 + 취소 검사. 3개는 전파가 전 인스턴스에 도달하는지 보기 위함 |
| MySQL | 1 | 취소 원장 |
| MinIO | 1 | 청크 객체 저장소 |

게이트웨이 뒤에 별도 백엔드를 두지 않음. 보호된 엔드포인트를 게이트웨이가 직접 응답함.
취소 전파를 배우는 데 프록시 계층이 더해주는 것이 없음.

## 3. 파라미터

원문 값을 1/60 로 축소함. 비율은 유지.

| 파라미터 | 원문 | 실습 | 근거 |
|---|---|---|---|
| 캐시 윈도우 | 12시간 | **12분** | 만료 동작을 실습 중에 관측 가능해야 함 |
| 청크 구간 | 30분 | **30초** | 윈도우/구간 = 24 유지 (청크 24개) |
| 쿠키 수명(갱신 주기) | 미공개 | **2분** | 윈도우의 1/6. 갱신 경로를 자주 타서 관측 가능하게 |
| 워커 폴링 주기 | 미공개 | **1초** | |
| 워커 배치 크기 | "수백 건" | **500건** | 원문과 같은 자릿수 |
| 게이트웨이 폴링 주기 | 미공개 | **1초** | |

예상 전파 지연: 워커 폴링(≤1s) + 처리(수십 ms) + 게이트웨이 폴링(≤1s) ≈ **2~3초**
7단계에서 실측함.

## 4. 레코드 포맷

16바이트 고정. 빅엔디언.

```
 byte  0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15
     ┌───────────────────────────────────────┬──────────────────────────┬─────────┐
     │          principal (uint64)           │  revoke_before (48bit)   │ flags   │
     │              8바이트                   │   6바이트, unix 밀리초     │ 2바이트  │
     └───────────────────────────────────────┴──────────────────────────┴─────────┘
```

| 필드 | 크기 | 의미 |
|---|---|---|
| `principal` | 8B | 사용자 ID |
| `revoke_before` | 6B | **이 시각 이전에 시작된 세션은 무효.** unix 밀리초. 2^48ms ≈ 서기 10889년 |
| `flags` | 2B | bit0 = LOGOUT. 나머지 예약 |

### 왜 빅엔디언인가

`principal` 이 최상위 바이트부터 오므로 **16바이트 레코드를 바이트 배열째로 비교하면 그 순서가 `(principal, revoke_before, flags)` 순서와 일치함.**

```go
bytes.Compare(recA, recB)   // 이것만으로 정렬·중복제거가 성립함
```

리틀엔디언이면 바이트 비교 순서가 값 순서와 어긋나서 별도 비교 함수가 필요함.

### 필드 이름

원문의 "login timestamp" 대신 `revoke_before` 를 씀. "이 시각 이전을 취소"라는 의미가 이름에서 바로 읽히기 때문.
쿠키 쪽의 `login_ts` 와 혼동하지 않기 위한 것이기도 함.

## 5. 청크

### 키

```
revocations/<구간 시작 unix 초, 10자리 0패딩>.bin

revocations/1755082800.bin    12:00:00 ~ 12:00:30
revocations/1755082830.bin    12:00:30 ~ 12:01:00
```

10자리 0패딩이면 문자열 정렬과 시간 정렬이 일치함 (서기 2286년까지 유효).
구간이 30초이므로 키의 초 값은 항상 30의 배수.

### 내용

16바이트 레코드의 flat 배열. `bytes.Compare` 오름차순 정렬. 완전 동일한 레코드는 중복 제거됨.

### 청크 배정 기준 — 생성 시각이 아니라 업로드 시각

**취소가 들어갈 청크는 "워커가 쓰는 시점"의 구간으로 정함.** 취소가 만들어진 시각이 아님.

원문은 "all revocations that were created within a particular 30-minute window" 라고 쓰지만,
전파 정확성을 생각하면 업로드 시각 기준이어야 함.

```
생성 시각 기준으로 하면:
  12:00:29 에 만들어진 취소가 워커 지연으로 12:00:35 에 처리됨
  → 12:00:00 청크(이미 지난 구간)를 수정해야 함
  → 그런데 게이트웨이는 최신 청크만 폴링하므로 이 변경을 못 봄
  → 취소가 영원히 전파되지 않음
```

업로드 시각 기준이면 항상 게이트웨이가 지켜보고 있는 최신 청크에 들어감. 그리고 **과거 청크는 절대 수정되지 않는다**는 성질이 생겨서:

- 게이트웨이 폴링 대상이 최신 2개로 한정됨 (전체 24개를 폴링할 필요 없음)
- 청크가 append-only 라는 불변식이 구간 단위로도 성립함

업로드 지연이 초 단위라 생성 시각과 업로드 시각의 차이는 무의미함.

## 6. MySQL 스키마

```sql
CREATE TABLE revocations (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  principal     BIGINT UNSIGNED NOT NULL,
  revoke_before BIGINT UNSIGNED NOT NULL,           -- unix 밀리초
  flags         SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  created_at    BIGINT UNSIGNED NOT NULL,           -- unix 밀리초
  uploaded_at   BIGINT UNSIGNED NULL,               -- NULL 이면 S3 미반영
  PRIMARY KEY (id),
  KEY idx_pending (uploaded_at, id),                -- 워커 스캔용
  KEY idx_principal (principal, revoke_before)      -- 쿠키 갱신 조회용
) ENGINE=InnoDB;
```

`uploaded_at` 이 워커의 진행 표시임. 워커는 `WHERE uploaded_at IS NULL ORDER BY id LIMIT 500` 으로 집어감.

**워커 여러 개가 같은 행을 집어가는 것을 막지 않음.** 막을 필요가 없음 —

1. 청크 병합이 중복 제거를 하므로 같은 레코드를 두 번 넣어도 결과가 같음 (idempotent)
2. 따라서 read-modify-write 가 멱등이고, 조건부 PUT 이 유실을 막아줌
3. 뒤늦게 처리한 워커는 병합 결과가 현재 청크와 동일해서 PUT 자체를 건너뜀 (7장 6단계)

행 잠금을 쓰지 않는 대신, 낭비되는 작업이 조금 생김. 워커 3개 규모에서는 무의미한 비용.

## 7. 워커 루프

```
1. SELECT * FROM revocations WHERE uploaded_at IS NULL ORDER BY id LIMIT 500
2. 없으면 1초 대기하고 1로
3. 현재 시각으로 청크 키 계산  key = keyFor(now)
4. 조건부 GET(key) → (body, etag).  404 면 body=빈, etag=없음
5. body 에 배치를 정렬 병합 + 중복 제거 → merged
6. merged == body 이면 PUT 생략하고 8로   (다른 워커가 이미 반영함)
7. PUT
     etag 있음 → If-Match: etag
     etag 없음 → If-None-Match: *
   412 면 백오프 후 4로 재시도 (상한 20회)
8. UPDATE revocations SET uploaded_at = now WHERE id IN (...)
9. 1로
```

### 병합 비용

배치 B건을 N건짜리 청크에 넣는 비용은 `O(B log B + N + B)`.
청크 하나의 수명 동안 총 비용은 `O(N² / B)` — 원문의 O(N²) 논의와 같음. B 가 분모에 있는 것이 배치의 효과.

30초 구간에 100만 건이 들어오는 규모가 아니므로 실습에서는 문제되지 않음.

### 재시도 정책

```
백오프 = 20ms × 2^attempt,  상한 500ms,  ±50% 지터
재시도 상한 20회 초과 시 에러 로그 남기고 다음 루프로 (해당 배치는 다음에 다시 집힘)
```

스파이크에서 워커 3개일 때 충돌이 드물게 나왔으므로 단순하게 시작함.
지터는 두 워커가 같은 주기로 부딪히는 것을 막기 위해 넣음.

## 8. 게이트웨이

### 8.1 부팅 시딩

```
1. cutoffStart = alignDown(now - 12분, 30초)
2. ListObjectsV2(Prefix="revocations/", StartAfter=keyFor(cutoffStart - 30초))
3. 반환된 키 중 구간 시작이 cutoffStart 이상인 것만 취함
4. 각 키를 GET → 메모리에 (구간시작 → {bytes, etag}) 로 보관
```

**이 경로에서 MySQL 을 건드리지 않음.** 성공 기준 3번이 이것.

### 8.2 요청 처리

```
1. 쿠키 복호화·인증 태그 검증 실패        → 401
2. 쿠키 expires 가 지났음                → 갱신 경로 (8.4)
3. 취소 검사 (8.3) 에서 걸림             → 401
4. 통과                                 → 200
```

1·2번은 쿠키 안의 정보만으로 판정됨. 3번만 청크가 필요함.

### 8.3 취소 검사

메모리에 있는 모든 청크에 대해 각각 이진 탐색.

```go
// 쿠키가 취소됐는지 검사
func isRevoked(c Cookie, chunks map[int64]Chunk) bool {
    for _, ch := range chunks {
        i := lowerBound(ch.Bytes, c.Principal)       // principal 이상인 첫 인덱스
        for ; i < recordCount(ch.Bytes); i++ {
            r := recordAt(ch.Bytes, i)
            if r.Principal != c.Principal {
                break                                 // 구간 끝
            }
            if r.Flags&FlagLogout != 0 && c.LoginTS < r.RevokeBefore {
                return true
            }
        }
    }
    return false
}
```

같은 principal 에 여러 건이 있을 수 있으므로 **첫 건에서 멈추지 않고 구간 전체를 순회함.**
`lowerBound` 는 레코드 앞 8바이트만 비교함.

비용: `O(청크 24개 × log(청크당 레코드 수))`.

### 8.4 갱신 경로

쿠키 수명(2분)이 지난 요청은 청크가 아니라 **MySQL 을 직접 조회**함.

```sql
SELECT 1 FROM revocations
WHERE principal = ? AND revoke_before > ?   -- ? = 쿠키의 login_ts
LIMIT 1
```

- 있으면 → 401
- 없으면 → 새 쿠키 발급. **`login_ts` 는 그대로 유지**하고 `expires` 만 갱신

`login_ts` 를 유지하는 것이 중요함. 갱신할 때 새 시각으로 바꾸면 이미 발생한 취소를 회피해버림.

이 경로가 12분 윈도우를 정당화함. 윈도우보다 오래된 취소는 이미 갱신 과정에서 걸렸으므로 메모리에 들고 있을 필요가 없음.

### 8.5 폴링

1초마다 **현재 구간과 직전 구간** 두 키에 조건부 GET.

```
304 → 아무것도 안 함
200 → 메모리의 해당 청크를 교체
404 → 아직 생성 안 됨. 무시
```

과거 청크는 5장의 배정 규칙 때문에 절대 수정되지 않으므로 폴링할 필요가 없음.
직전 구간까지 보는 이유는 구간 경계에서 워커가 조금 늦게 쓸 수 있기 때문.

**304 는 정상 경로임.** AWS SDK 가 304 를 에러로 반환하므로 상태 코드로 판별해야 함
([02-spike-minio.md](02-spike-minio.md) A절 참고). 이걸 놓치면 매초 에러 로그가 쌓임.

### 8.6 만료

폴링 때마다 함께 수행. 구간의 **끝**이 `now - 12분` 보다 오래된 청크를 메모리에서 제거.
MinIO 의 객체는 지우지 않음.

### 8.7 시계

워커와 게이트웨이가 각자 자기 시계로 구간 키를 계산함. 도커 컴포즈에서 전부 같은 호스트 시계를 쓰므로 어긋나지 않음.
7단계에서 전파 지연을 잴 때도 이 사실에 기댐.

## 9. 쿠키

AES-256-GCM. 평문 24바이트.

```
 byte  0        8        16       24
       ├────────┼────────┼────────┤
       │principal│login_ts│expires │
       │ uint64 │ uint64 │ uint64 │      unix 밀리초
       └────────┴────────┴────────┘
```

```
쿠키 값 = base64url( nonce(12B) || ciphertext(24B) || tag(16B) )   = 52B → 70자
```

- 건마다 랜덤 nonce. AES-GCM 은 nonce 재사용 시 치명적이므로 반드시 매번 생성
- 키는 환경변수로 주입. auth 와 gateway 가 공유
- 인증 태그 검증 실패 = 위조 → 즉시 401

`login_ts` 는 세션이 시작된 시각이고 **갱신해도 바뀌지 않음.** `expires` 만 갱신됨.

## 10. API

### auth (:8080)

| 메서드 | 경로 | 동작 |
|---|---|---|
| POST | `/login` | body `{"user_id": 1002}` → `Set-Cookie: session=...` |
| POST | `/logout` | 쿠키의 principal 로 취소 INSERT (`revoke_before = now`) |

### gateway (:8081, :8082, :8083)

| 메서드 | 경로 | 동작 |
|---|---|---|
| GET | `/api/me` | 보호된 엔드포인트. 200 또는 401 |
| GET | `/debug/chunks` | 보유 청크 목록 (구간, 레코드 수, ETag) — 성공 기준 5번 확인용 |
| GET | `/debug/stats` | 시딩 시간, 폴링 횟수, 304 횟수, MySQL 조회 횟수 |

`/debug/*` 는 E2E 검증과 7단계 측정을 위해 **설계 단계에서 미리 넣음.** 나중에 붙이려면 관측 지점이 없어서 곤란해짐.

## 11. 모듈 레이아웃

```
go.mod                          루트 모듈
cmd/auth/main.go
cmd/gateway/main.go
cmd/worker/main.go
internal/revocation/            레코드 포맷, 이진 탐색, 정렬 병합, 중복 제거
internal/chunkstore/            S3 조건부 GET/PUT 래퍼, 304·412 판별 헬퍼
internal/session/               쿠키 암복호화
internal/store/                 MySQL 접근
internal/chunkkey/              구간 정렬·키 생성
docker-compose.yml              mysql, minio, auth, worker×3, gateway×3
migrations/001_revocations.sql
scripts/e2e.sh                  6단계 성공 기준 검증
```

`spike/` 는 별도 모듈로 그대로 둠. 버릴 코드이므로 본 모듈과 섞지 않음.

## 12. 구현 순서

앞 단계가 테스트를 통과해야 다음으로 감.

| # | 대상 | 검증 |
|---|---|---|
| 1 | `internal/revocation` | 단위테스트: 인코딩 왕복, 정렬 불변식, 중복 제거, 같은 principal 다건 조회, 경계값 |
| 2 | `internal/chunkkey` | 단위테스트: 정렬, 키 생성, 문자열 정렬 = 시간 정렬 |
| 3 | `internal/chunkstore` | MinIO 상대 통합테스트: 412·304·404 경로 |
| 4 | `internal/session` | 단위테스트: 왕복, 위조 탐지, nonce 매번 다름 |
| 5 | `internal/store` + 마이그레이션 | MySQL 상대 통합테스트 |
| 6 | `cmd/worker` | 워커 3개 동시 실행에서 유실 0 |
| 7 | `cmd/gateway` | 시딩 → 조회 → 폴링 → 만료 |
| 8 | `cmd/auth` | 로그인·로그아웃 |
| 9 | `docker-compose.yml` | `docker compose up` 한 번으로 전체 기동 |

## 13. 성공 기준 (6단계에서 판정)

`scripts/e2e.sh` 가 통과/실패를 출력함.

| # | 기준 | 관측 방법 |
|---|---|---|
| 1 | 로그인 → `/api/me` 200. 쿠키 1바이트 변조 → 401 | 상태 코드 |
| 2 | 로그아웃 → 게이트웨이 **3개 전부**가 3초 내 401 | 8081·8082·8083 에 각각 직접 요청. 로드밸런서 뒤에서 한 번만 쏘면 나머지가 아직 모르는 것을 놓침 |
| 3 | 게이트웨이 재시작 후에도 2번이 성립하고, 시딩 중 MySQL 쿼리 0건 | `/debug/stats` 의 MySQL 조회 횟수 + MySQL general log |
| 4 | 워커 3개 동시 실행 + 취소 10,000건 → 청크에서 읽히는 건수 정확히 10,000 | 청크 전체 파싱 후 카운트. 조건부 PUT 을 끈 대조 1회 포함 |
| 5 | 12분 지난 청크가 게이트웨이 메모리에서 사라짐 | `/debug/chunks` 의 청크 목록 변화 |

4번의 대조 실행은 "내가 넣은 안전장치가 실제로 일하는지" 확인용임. 원문 검증이 아님.

## 14. 7단계에서 측정할 것

검증이 아니라 내 시스템의 특성 기록.

| 지표 | 방법 |
|---|---|
| 전파 지연 p50/p99 | 로그아웃 시각 → 각 게이트웨이가 401 을 주기 시작한 시각. 폴링 주기를 0.5/1/5초로 바꿔가며 |
| 부팅 시딩 | 청크 수, 전송 바이트, 소요 시간 |
| 메모리 | 취소 N건일 때 게이트웨이 힙. `runtime.GC()` 후 `HeapAlloc` |
| 워커 처리량 | 배치 크기별 초당 처리 건수, CPU 시간 대 S3 대기 시간 비율 |
| 조회 지연 | 이진 탐색 p50/p99 |
| 조건부 GET 절감 | 304 비율과 절감 바이트 ([02-spike-minio.md](02-spike-minio.md) 에서 미측정으로 남긴 항목) |

## 15. 원문과 다르게 한 것

| 항목 | 원문 | 실습 | 이유 |
|---|---|---|---|
| 리더 선출 | ZooKeeper | **없음** | 원문도 최적화라고 명시. 스파이크에서 조건부 PUT 만으로 유실 0 확인 |
| 취소 타입 | 다수 (로그아웃, 정보 무효화, 조직 단위) | 로그아웃 1종 | 플래그 비트 자리만 예약. 전파 메커니즘 학습에 추가되는 것이 없음 |
| 청크 배정 | 생성 시각 | **업로드 시각** | 5장. 전파 누락을 막기 위함 |
| 게이트웨이 폴링 범위 | "the latest chunks" | 최신 2개 | 과거 청크가 수정되지 않으므로 |
| 워커 행 선점 | 미공개 | 선점 없음 | 병합이 멱등이라 불필요 |
| 스케일 | 12시간 / 30분 | 12분 / 30초 | 만료를 관측 가능하게 |
| 언어 | Java | Go | 바이너리 처리·벤치마크 도구 |
| 저장소 | S3 | MinIO | 로컬, 무비용 |
