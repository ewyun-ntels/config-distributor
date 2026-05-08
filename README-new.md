# Config Distributor

ConfigMap 데이터를 REST API로 받아 NATS JetStream KV에 저장하는 단일 writer 서비스.

## 설계 기준

- **NATS JetStream KV가 source of truth**
- ConfigMap은 최초 시드(seed) + 백업 미러로만 사용 (KV에 의존성을 두지 않음, ConfigMap 단독으로는 운영 갱신 경로 아님)
- 모든 변경은 REST API를 통해서만 KV에 반영
- 별도 백그라운드 reconciler가 **KV → ConfigMap** 단방향으로 주기적 미러링 (best-effort, 쓰기 경로와 분리)
- REST API 쓰기 경로는 k8s-api-server에 의존하지 않음 (reconciler가 비동기로만 사용)

## 부트스트랩 정책

판정 기준: KV에 sentinel 키 `__bootstrap_done__` 존재 여부 + 일반 KV 키 존재 여부의 조합

| sentinel | 일반 키 | 동작 |
| --- | --- | --- |
| 있음 | 무관 | **skip** (이미 부트스트랩 완료) |
| 없음 | 없음 | **부트스트랩 수행** (최초 배포) |
| 없음 | 있음 | **fail-fast** (이전 버전 데이터 또는 외부 주입 의심 — 수동 마이그레이션 필요) |

- 부트스트랩 성공 후 sentinel 키를 기록 → 이후 재시작 시 일반 키가 일부 비더라도 다시 시드하지 않음
- 부트스트랩 이후 ConfigMap에 직접 가한 수정은 **반영되지 않으며**, reconciler가 다음 사이클에 KV 값으로 **덮어씀** (운영 변경은 REST API만 사용)
- 부트스트랩 실패 시 **fail-fast** (프로세스 종료, 재시작에 맡김)
- 의도: 최초 배포 시 다량의 기존 설정을 REST API로 일일이 밀어넣는 부담을 줄이기 위함. sentinel 없음 + 일반 키 있음을 fail-fast 처리하는 이유는 기존 KV 데이터가 ConfigMap 시드로 덮어써지는 운영 사고 방지

## ConfigMap Reconciler (KV → ConfigMap 단방향 미러링)

REST API 쓰기 경로와 분리된 백그라운드 워커. 주기적으로 KV의 각 키를 ConfigMap에 반영합니다.

- **방향**: KV → ConfigMap 단방향, **create/update만** 수행 (delete 미지원)
- **주기**: 기본 5분 (`reconciler.intervalSeconds` 로 설정 가능)
- **Best-effort**: k8s-api 실패는 로그/메트릭만 기록, 다음 사이클에서 재시도. REST API 응답에는 영향 없음
- **드리프트 보정**: 누군가 ConfigMap을 직접 수정해도 다음 사이클에 KV 값으로 복원
- **신규 ConfigMap 자동 생성**: REST API로 새 키가 KV에 들어오면 다음 사이클에 ConfigMap 생성 (관리 라벨 자동 부착)
- **단일 키 계약 유지**: ConfigMap 생성 시 `data: { value: <KV value> }` 형태로 단일 키 사용
- **namespace 기준**: reconciler는 `bootstrap.namespaces`를 순회하지 않고, KV key(`namespaces/{namespace}/configmap/{name}`)에 들어있는 namespace를 그대로 사용

→ "운영 중 새 config 추가" 시나리오는 REST API 호출 한 번이면 충족. ConfigMap은 reconciler가 알아서 만들어줌.

### 삭제 처리 (운영 절차)

reconciler는 **PUT/UPDATE는 self-heal하지만 DELETE는 self-heal 대상이 아닙니다** (의도적 단순화).

**Config 삭제 시 운영자 필수 절차**:

1. REST API DELETE 호출 → KV에서 삭제
2. 백업 미러 ConfigMap을 운영자가 **수동으로 삭제** (`kubectl delete configmap ...`)

**2단계를 누락하면 발생하는 문제**:
- ConfigMap이 zombie 상태로 남음
- 향후 KV 버킷 손상 등으로 재부트스트랩이 일어날 때 zombie ConfigMap에서 **삭제했던 config가 부활**
- 운영 사고로 이어질 수 있으므로 1+2를 한 세트로 취급해야 함

이는 1차에서 코드 단순성을 우선한 결정. 빈번한 삭제가 발생하는 운영 패턴이 확인되면 향후 reconciler에 삭제 전파 추가 검토.

## 시작 동작

1. NATS 연결 및 KV 버킷 확보
2. sentinel 키 존재 여부와 일반 KV 키 존재 여부를 함께 확인
3. **sentinel 있음** → 부트스트랩 스킵 → cache preload (KV → cache) → REST API + reconciler 시작
4. **sentinel 없음 + 일반 키 없음** → kube client 생성(lazy) → 라벨 필터에 매칭되는 ConfigMap List → KV에 시드 → sentinel 기록 → cache preload → REST API + reconciler 시작
5. **sentinel 없음 + 일반 키 있음** → fail-fast (수동 마이그레이션 필요)
6. 부트스트랩이 스킵되는 경로에서는 k8s-api-server 가용성과 무관하게 기동 가능 (REST API는 항상 영향 없음, reconciler 동작은 아래 "kube client 실패 처리" 참조)

### reconciler kube client 실패 처리

두 가지 실패 모드를 구분합니다:

| 상황 | 동작 |
| --- | --- |
| **kubeconfig / in-cluster config 자체가 없음** (구성 누락) | reconciler **영구 비활성화** (로그 + 메트릭만 기록, 재시도 안 함) |
| **kube client는 생성됐지만 k8s-api 호출 실패** (네트워크 단절, api-server down, 권한 문제) | 매 사이클마다 **retry**. 회복되면 자동 정상화 |

어느 경우든 **REST API 응답에는 영향 없음**.

## ConfigMap → KV value 변환 규칙 (단일 키 계약)

bootstrap 대상 ConfigMap은 **`data`에 단일 키만** 가져야 합니다.

- 단일 키 → 그 값을 **그대로** KV에 저장 (키 이름은 무관)
- 다중 키 → 경고 로그 후 **skip** (시드하지 않음)
- 비어있음(0개 키) → skip
- 키 이름을 강제하지 않으므로 `value`, `config`, `application.yaml` 등 기존 운영 관례를 그대로 수용 가능

이 계약의 효과:
- REST API 본문 ≡ KV value ≡ ConfigMap의 단일 키 값 (1:1:1 대응)
- 소비자(Pod)는 KV value를 항상 동일한 형식으로 파싱
- 다중 키가 필요한 운영자는 본인이 JSON/YAML 등으로 직렬화해 단일 키에 넣음

### 키 이름 정규화

- **bootstrap**: 단일 키이기만 하면 키 이름 무관 (`value`, `config`, `application.yaml` 등 모두 허용)
- **reconciler**: 관리 라벨이 붙은 ConfigMap을 update할 때 키 이름을 **`value`로 정규화**
  - 예: 기존 ConfigMap이 `data: { application.yaml: "..." }` 였더라도 reconciler가 한 번 update하면 `data: { value: "..." }` 형태로 바뀜
  - 정규화는 의도된 동작. 운영 단계에서는 모든 관리 ConfigMap이 `data.value` 키 사용으로 통일됨
  - 같은 namespace/name의 비관리 ConfigMap이 이미 있으면 덮어쓰지 않고 conflict metric/log만 남김

### 값 표현 범위 (text config 전제)

- 본 시스템은 **텍스트 config**를 전제로 합니다 (YAML, JSON, properties, plain text 등)
- ConfigMap의 `data` 필드는 사실상 string 저장소이며, 임의 **binary payload는 지원하지 않습니다**
  - binary 값이 필요한 운영자는 base64 등으로 텍스트 인코딩해서 사용
- Secret은 제거했으므로 binary/암호화된 데이터는 본 시스템의 범위 밖

> 기존 envelope 포맷(`storedValueEnvelope`)은 폐기.

## Secret

- 본 도구에서는 **다루지 않음** (의도적으로 제외)

## 메트릭

- `/metrics`에서 REST API 요청 처리, KV 작업, 부트스트랩 결과, reconciler 실행 결과를 Prometheus 텍스트 포맷으로 노출

## 요구 사항

- JetStream 활성화된 NATS 서버
- 접근 가능한 Kubernetes API server (부트스트랩 시점 + reconciler가 사용 — reconciler는 best-effort라 일시 단절 허용)

## 라벨 필터 (부트스트랩 시드 대상 식별)

- `config.upm.io/managed: "true"` 라벨이 붙은 ConfigMap만 부트스트랩 시드 후보
- `config.yaml`의 `filter.managedLabel`로 키/값 변경 가능
- 부트스트랩 이후에는 라벨이 사용되지 않음

## 실행 예시

```bash
# NATS 서버 (예시)
# nats-server -js

# Distributor 실행 (config.yaml 사용)
go run ./cmd/distributor config.yaml

# 예시 요청
curl -X PUT http://localhost:8080/namespaces/default/configmap/appA \
  -H 'Content-Type: text/plain' \
  --data-binary $'app: appA\nversion: 1\n'

curl http://localhost:8080/namespaces/default/configmap/appA
curl http://localhost:8080/namespaces/default/configmap
curl http://localhost:8080/namespaces/all/configmaps
```

## 가용성

- REST API 쓰기 경로는 k8s-api-server 가용성에 의존하지 않음 (KV에만 직접 씀)
- 부트스트랩이 이미 완료된 상태(sentinel 존재)에서 재시작하면 k8s-api-server가 다운되어 있어도 distributor는 정상 기동, REST API도 정상 동작
- reconciler 실패는 위 "kube client 실패 처리" 표 참조: 구성 누락은 영구 비활성화, API 호출 실패는 사이클별 retry. 어느 쪽이든 REST API에는 영향 없음

## 재해복구

- KV 데이터 보호는 NATS JetStream 자체의 복제(replicas) 및 스트림 백업/복원(`nats stream backup/restore`)에 위임
- KV 버킷이 완전히 손상되어 sentinel 포함 비워지는 경우, 재시작 시 ConfigMap의 **마지막 reconciler 사이클까지의 상태**로 시드. reconciler 주기(기본 5분) 이내의 최근 변경은 유실 가능
- reconciler가 KV → ConfigMap을 지속적으로 미러링하므로, 평상시 ConfigMap은 KV의 최근 스냅샷에 가까운 상태를 유지

## 향후 옵션 (1차 스펙 미포함)

- per-write 동기 트리거(REST API PUT 직후 즉시 export) — latency를 5분에서 초 단위로 단축. 1차에는 periodic만 두고, 실측 후 필요성이 확인되면 추가
- reconciler에 gauge 메트릭 (마지막 동기화 시각, drift 키 개수 등) — metrics 패키지에 gauge 지원 추가 후 도입

---

# 구현 변경 가이드 (개발 담당자용)

본 섹션은 위 설계로 전환하기 위해 현재 코드베이스에서 변경이 필요한 부분을 정리합니다.

## 확정된 정책

| # | 항목 | 결정 |
| --- | --- | --- |
| 1 | KV value 포맷 | envelope 폐기, **raw value (text)** 그대로 저장. binary payload 미지원 |
| 2 | ConfigMap 변환 규칙 | **단일 키 계약**. 다중 키 ConfigMap은 skip |
| 3 | 부트스트랩 실패 처리 | **fail-fast** |
| 4 | Secret | **전면 제거** |
| 5 | 부트스트랩 멱등성 | KV에 **sentinel 키 `__bootstrap_done__`** 기록·확인 |
| 6 | LIST 응답 | 시작 시 **KV → cache preload** (GET/LIST 모두 cache 우선) |
| 7 | kube client 초기화 | **부트스트랩 lazy + reconciler 시작 시 생성**. REST API는 kube 미의존 |
| 8 | 설정 스키마 | `watch` → `bootstrap`, `resources` 필드 제거, `reconciler` 추가 |
| 9 | KV → ConfigMap 미러링 | **periodic reconciler** (기본 5분, best-effort, create/update만, **delete 미지원**) |

## 변경의 핵심 요약

| 항목 | 현재 | 변경 후 |
| --- | --- | --- |
| Source of Truth | Kubernetes (informer watch) | NATS JetStream KV |
| K8s → KV 동기화 | informer 이벤트 기반 (상시) | 부트스트랩 시 1회만 (sentinel 미존재 시) |
| KV → K8s 동기화 | 없음 (역방향) | **periodic reconciler** (5분 주기, create/update만, best-effort) |
| REST API 쓰기 경로 | K8s에 먼저 쓰고 KV에도 씀 (이중 쓰기) | KV에만 씀 (k8s-api 미의존) |
| Secret 처리 | 지원 | 미지원 (전면 제거) |
| kube client 생성 | 시작 시 무조건 | 부트스트랩 시점 (필요 시) + reconciler가 별도 보유 |
| KV value 포맷 | envelope JSON | raw value (text) — binary 미지원 |
| LIST 데이터 출처 | in-memory cache only | cache (KV preload로 채움) |

## 1. 제거 대상

### 1-1. K8s informer 기반 Watch 로직 전체
- [internal/apiserver/apiserver.go](internal/apiserver/apiserver.go) `startKubeWatchers` 함수 제거 (informer factory, AddEventHandler, WaitForCacheSync 등)
- 같은 파일의 이벤트 핸들러 전체 제거
  - `handleConfigMapUpsert`, `handleConfigMapDelete`
  - `handleSecretUpsert`, `handleSecretDelete`
  - `extractConfigMap`, `extractSecret`
- `Run` 내부의 `s.startKubeWatchers(ctx)` 호출 제거 → 부트스트랩 + cache preload 호출로 대체
- `reconcileNamespaceFromStore`, `deleteStaleKVKeys`, `isWatchedKey` 제거
- `putIfChangedByRV` 제거 (informer-driven RV 비교 분기 불필요)
- 관련 import 정리: `k8s.io/client-go/informers`, `k8s.io/client-go/tools/cache` 등

### 1-2. Secret 관련 코드 전면 제거
- [internal/apiserver/apiserver.go](internal/apiserver/apiserver.go): `resourceSecret` 상수, secret 분기
- [internal/api/resources/v1alpha1/handler.go](internal/api/resources/v1alpha1/handler.go): `listSecrets`, `getSecret`, `putSecret`, `deleteSecret` 메서드
- [internal/api/resources/v1alpha1/register.go](internal/api/resources/v1alpha1/register.go): `/secret` 라우트 5종 제거
- [internal/kube/client.go](internal/kube/client.go): `UpsertSecret`, `DeleteSecret`, `IsManagedSecret`, `ValueFromSecret`, `parseSecretValue`, `secretEnvelope`, `secretValues` 타입 제거
- [internal/config/config.go](internal/config/config.go) `setDefaults`의 resources 기본값에서 `"secrets"` 제거 (필드 자체 제거)
- [config.yaml](config.yaml) `watch.resources` 제거

### 1-3. REST API 쓰기 경로의 K8s write-back 제거
- [internal/api/resources/v1alpha1/handler.go](internal/api/resources/v1alpha1/handler.go) `putItem`: `applyToKube` 호출 제거. KV에 직접 Put → cache upsert → 응답
- 같은 파일 `deleteItem`: `deleteFromKube` 호출 제거. KV Delete → cache Delete → 응답
- `applyToKube`, `deleteFromKube` 함수 자체 제거
- 응답 페이로드에서 `k8sResourceVer` 필드 제거
- Handler에서 `kube.Client` 의존성 제거 → 생성자 시그니처 정리

### 1-4. Envelope 포맷 폐기
- [internal/kube/client.go](internal/kube/client.go) `wrapStoredValue`, `extractStoredPayload`, `storedValueEnvelope`, `storedMetadata`, `configMapEnvelope`, `parseConfigMapValue`, `ExtractResourceVersion` 제거
- `ValueFromConfigMap`은 단일 키 추출 함수로 재작성 (아래 2-1 참조)

## 2. 신규 추가 / 변경

### 2-1. `ValueFromConfigMap` 단순화
[internal/kube/client.go](internal/kube/client.go):
```go
// ConfigMap.Data 가 정확히 1개 키를 가질 때 그 값을 그대로 반환.
// 0개 또는 2개 이상이면 ok=false (호출자는 skip + 경고 로그).
// 키 이름은 검증하지 않음 (value, config, application.yaml 등 모두 허용).
func ValueFromConfigMap(cm *corev1.ConfigMap) (value string, ok bool)
```

### 2-2. 부트스트랩 함수 신설
[internal/apiserver/apiserver.go](internal/apiserver/apiserver.go):

동작 의사코드:
```
const sentinelKey = "__bootstrap_done__"

bootstrapIfNeeded(ctx):
    // --- 3-state 판정 ---
    sentinelExists := kv.Get(sentinelKey) 성공 여부
                      (ErrKeyNotFound = false, 그 외 에러 = fail-fast)
    hasNormalKeys  := kv.Keys()에 sentinelKey 외 키가 1개라도 존재
                      (ErrNoKeysFound = false, 그 외 에러 = fail-fast)

    if sentinelExists:
        return  // 이미 부트스트랩 완료
    if hasNormalKeys:
        fail-fast("KV has data without sentinel; manual migration required")
    // 여기 도달 = sentinel 없음 + 일반 키 없음 → 시드 진행

    // --- kube client lazy 생성 ---
    kubeClient, err := kube.NewClient(managedLabel)
    if err: fail-fast

    // --- 시드 ---
    for ns in cfg.Bootstrap.Namespaces:
        list := kubeClient.ClientSet().CoreV1().ConfigMaps(ns).
            List(ctx, metav1.ListOptions{LabelSelector: kubeClient.ManagedLabelSelector()})
        for cm in list.Items:
            value, ok := kube.ValueFromConfigMap(&cm)
            if !ok:
                log.Warn("skip multi-key or empty configmap", "ns", ns, "name", cm.Name)
                metrics.IncBootstrapSkipped(ns, "multi_key_or_empty")
                continue
            kv.Put(store.KeyFor(ns, "configmap", cm.Name), []byte(value))
            metrics.IncBootstrapSeeded(ns, "ok")

    kv.Put(sentinelKey, []byte(time.Now().Format(time.RFC3339)))
```

> 이 함수는 KV 쓰기와 메트릭만 담당. **cache는 채우지 않음** — 직후 호출되는 `preloadCacheFromKV`에서 일관되게 채움.

### 2-3. KV → cache preload 함수 신설
[internal/apiserver/apiserver.go](internal/apiserver/apiserver.go):
```
preloadCacheFromKV():
    keys := kv.Keys()
    for k in keys:
        if k == sentinelKey: continue
        ns, kind, name, ok := store.ParseKey(k)
        if !ok: continue
        entry := kv.Get(k)
        cache.Upsert(ns, kind, name, {Revision: entry.Revision(), Value: string(entry.Value())})
```
- `Run()` 흐름: `bootstrapIfNeeded` → `preloadCacheFromKV` → HTTP 서버 시작
- namespace LIST(`listByPrefix`)와 전체 LIST(`/namespaces/all/configmaps`)는 cache만 봐도 됨 (preload로 채워졌으니 누락 없음)

### 2-4. 핸들러 단순화
[internal/api/resources/v1alpha1/handler.go](internal/api/resources/v1alpha1/handler.go):
- `putItem`: 본문 bytes를 그대로 `kv.Put` → `cache.Upsert` → revision 응답
- `deleteItem`: `kv.Delete` → `cache.Delete` → 204
- `kube.Client` 의존성 / `applyToKube` / `deleteFromKube` / `k8sResourceVer` 응답 필드 모두 제거
- 생성자 `NewHandlerWithDeps(kv, cache)` — kube 파라미터 제거

### 2-5. kube client 초기화 (lazy + reconciler 별도)
[internal/apiserver/apiserver.go](internal/apiserver/apiserver.go) `New()`:
- 현재 `New()`의 무조건 `kube.NewClient` 호출 **제거**
- 부트스트랩 단계에서 sentinel 미존재 시 lazy 생성 (기존 2-2 의사코드 참조)
- reconciler는 자체적으로 kube client 보유. 두 가지 실패 모드를 구분 처리:
  - **kube client 생성 실패** (kubeconfig/in-cluster config 자체 없음): reconciler **영구 비활성화** (재시도 없이 로그 + `reconciler_runs_total{result="disabled"}` 1회 기록 후 goroutine 종료)
  - **k8s-api 호출 실패** (생성은 됐는데 API 요청이 실패): 매 사이클 retry. 회복 시 자동 정상화
- 어느 경우든 REST API는 정상 기동 (kube 미의존)
- `APIServer` 구조체의 `kube` 필드는 제거 (REST API 본체에는 필요 없음)

### 2-6. 설정 스키마 정리
[internal/config/config.go](internal/config/config.go), [config.yaml](config.yaml):
- `WatchConfig` → `BootstrapConfig`로 리네임
- `Bootstrap.Resources` 필드 제거 (configmap 외 지원 안 함)
- `Bootstrap.Namespaces`만 유지
- `filter.managedLabel`은 유지
- 신규 `ReconcilerConfig` 추가
  - `Enabled` (bool, 기본 true)
  - `IntervalSeconds` (int, 기본 300)

변경 후 `config.yaml` 예:
```yaml
log:
  level: info

server:
  port: 8080

nats:
  url: nats://10.255.254.22:4222
  bucket: UPM_CONFIG

bootstrap:
  namespaces:
    - default

reconciler:
  enabled: true
  intervalSeconds: 300

filter:
  managedLabel:
    key: config.upm.io/managed
    value: "true"
```

### 2-7. 메트릭 정리
[internal/apiserver/apiserver.go](internal/apiserver/apiserver.go), [internal/metrics/metrics.go](internal/metrics/metrics.go):
- `cfg_distributor_kube_events_total` 제거 (K8s 이벤트 없음)
- `cfg_distributor_kv_operations_total`은 유지하되 `resource` 라벨에서 `secret` 제거
- 신설 (counter만, gauge는 1차 미포함):
  - `cfg_distributor_bootstrap_seeded_total{namespace,result}`
  - `cfg_distributor_bootstrap_skipped_total{namespace,reason}` (`multi_key_or_empty` 등)
  - `cfg_distributor_reconciler_runs_total{result}` (`success` / `error` / `disabled` — disabled는 kube client 생성 실패로 영구 비활성화된 경우 1회 기록)
  - `cfg_distributor_reconciler_actions_total{namespace,action,result}` (action: `create`/`update`/`noop`/`conflict` — delete는 미지원)

> 현재 [metrics.go](internal/metrics/metrics.go)는 counter만 지원하므로 1차 구현 범위를 작게 유지하기 위해 gauge(`bootstrap_status`, reconciler `last_run_timestamp`, drift 키 개수 등)는 제외. 필요해지면 metrics 패키지에 gauge 지원을 추가하면서 함께 도입.

### 2-8. ConfigMap Reconciler 구현 (신규 패키지)
신규: `internal/reconciler/reconciler.go`

동작 의사코드:
```
type Reconciler struct {
    kv          nats.KeyValue
    kubeClient  *kube.Client
    interval    time.Duration
}

Run(ctx):
    // [실패 모드 1] kube client 생성 자체가 실패한 경우 → 영구 비활성화
    if kubeClient == nil:
        log.Warn("reconciler disabled: kube client unavailable (kubeconfig missing)")
        metrics.IncReconcilerRun("disabled")
        return  // goroutine 종료, retry 없음

    // [실패 모드 2] kube client는 있으나 매 호출마다 실패할 수 있음 → runOnce 내부에서 사이클별 retry
    ticker := time.NewTicker(interval)
    runOnce(ctx)  // 첫 사이클 즉시 실행
    for {
        select <-ticker.C: runOnce(ctx)
        select <-ctx.Done: return
    }

runOnce(ctx):
    keys := kv.Keys()
    for k in keys:
        if k == sentinelKey: continue
        ns, kind, name, ok := store.ParseKey(k)
        if !ok || kind != "configmap": continue
        entry := kv.Get(k)
        value := string(entry.Value())

        // Get → 없으면 Create, 있고 단일 키 값이 다르면 Update, 같으면 noop
        // 같은 namespace/name의 비관리 ConfigMap이 있으면 conflict로 기록하고 skip
        // 실패 시 로그 + metric, 다음 키 계속 (best-effort, 다음 사이클에서 재시도)
        kubeClient.UpsertConfigMapSingleKey(ctx, ns, name, value)

    metrics.IncReconcilerRun("success" 또는 "error")
```
- `kube.Client`에 `UpsertConfigMapSingleKey(ctx, ns, name, value)` 헬퍼 추가 (단일 키 계약 + 관리 라벨 자동 부착, 변경 없으면 API 호출 생략)
- 기존 `UpsertConfigMap`을 단일 키 계약에 맞게 재작성 또는 위 헬퍼로 대체
- reconciler는 KV key의 namespace를 그대로 사용하므로, REST API로 `bootstrap.namespaces` 외 namespace에 쓴 값도 미러링 대상이 됨
- 비관리 ConfigMap conflict는 덮어쓰지 않음. action=`conflict`, result=`error`로 기록하고 run 자체는 계속 진행
- **삭제 로직 없음** (의도적 단순화 — 위 "삭제 처리(제한사항)" 섹션 참조)

> reconciler는 `cmd/distributor/main.go` 또는 `apiserver.Run()`에서 별도 goroutine으로 시작. 컨텍스트 취소로 정상 종료.

## 3. RBAC / 배포 영향

[helm/distributor](helm/distributor) 의 ClusterRole/Role 권한:
- **ConfigMap**: `get`, `list`, `create`, `update` 필요
  - `get`/`list` → 부트스트랩 + reconciler 조회
  - `create`/`update` → reconciler 미러링
  - `watch`, `patch`, `delete` 권한은 모두 제거 (informer 제거 + reconciler 삭제 미지원)
- **Secret**: 권한 전체 제거

## 4. 관련 파일 영향 정리 (체크리스트)

- [ ] [cmd/distributor/main.go](cmd/distributor/main.go) — reconciler goroutine 시작 추가 (또는 apiserver.Run에서 시작)
- [ ] [internal/apiserver/apiserver.go](internal/apiserver/apiserver.go) — informer/이벤트 핸들러 전체 제거, 부트스트랩 + cache preload 함수 추가, kube client APIServer 보유 제거
- [ ] [internal/api/resources/v1alpha1/handler.go](internal/api/resources/v1alpha1/handler.go) — K8s write-back 제거, secret 메서드 제거, kube 의존성 제거
- [ ] [internal/api/resources/v1alpha1/register.go](internal/api/resources/v1alpha1/register.go) — secret 라우트 제거
- [ ] [internal/api/resources/v1alpha1/types.go](internal/api/resources/v1alpha1/types.go) — secret 관련 타입 / `k8sResourceVer` 응답 필드 제거
- [ ] [internal/kube/client.go](internal/kube/client.go) — secret 함수 및 envelope 제거, `ValueFromConfigMap`을 단일 키 추출로 재작성, `UpsertConfigMapSingleKey` 헬퍼 추가, `DeleteConfigMap` 제거(reconciler 삭제 미지원)
- [ ] **[internal/reconciler/reconciler.go](internal/reconciler/reconciler.go) — 신규 추가**
- [ ] [internal/config/config.go](internal/config/config.go) — `Watch` → `Bootstrap`, `Resources` 필드 제거, `ReconcilerConfig` 추가
- [ ] [config.yaml](config.yaml) — 스키마 변경 반영 (`reconciler` 추가)
- [ ] [internal/metrics/metrics.go](internal/metrics/metrics.go) — bootstrap + reconciler 관련 counter 추가
- [ ] [helm/distributor](helm/distributor) — RBAC: ConfigMap `get/list/create/update`만, secret 권한 제거
- [ ] [example](example) — Secret 예시 제거, ConfigMap 예시는 단일 키로 정리
- [ ] [spec/openapi](spec/openapi) — Secret 엔드포인트 명세 제거, 응답 스키마에서 `k8sResourceVer` 제거

## 5. 검증

- 테스트 파일은 현재 없음. 기존 컴파일은 `env GOCACHE=/tmp/codex-gocache go test ./...` 로 통과 확인됨
- 변경 후에도 같은 명령으로 빌드/테스트 통과 확인
- 수동 검증 시나리오:
  1. **최초 배포**: KV 빈 상태 + ConfigMap 있음 → 시작 시 시드 확인, sentinel 기록 확인
  2. **재시작 (sentinel 존재)**: 시드 스킵 + k8s-api 차단해도 distributor 기동/REST API 정상 응답 확인
  3. **REST API PUT**: KV/cache 반영, GET/LIST 정상 응답
  4. **다중 키 ConfigMap (부트스트랩)**: skip 로그 + skipped_total 메트릭 카운트 확인
  5. **reconciler 신규 생성**: 시드 없이 시작 후 REST API로 신규 키 PUT → 다음 사이클(또는 짧은 interval로 테스트)에 ConfigMap 생성 + 관리 라벨 부착 확인
  6. **reconciler drift 보정**: `kubectl edit configmap` 으로 값 직접 변경 → 다음 사이클에 KV 값으로 복원 확인
  7. **reconciler 삭제 미지원 (zombie)**: REST API DELETE → KV에서는 삭제, ConfigMap은 남아있는지 확인 (의도된 동작)
  8. **reconciler best-effort**: k8s-api 차단 상태에서 REST API PUT 정상 응답 확인 (reconciler만 retry)
  9. **3-state fail-fast**: KV에 sentinel 없이 일반 키만 넣은 상태로 distributor 재시작 → 시작 거부 확인
