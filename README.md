# JetStream KV Config 전파 예제

구성
- Config Distributor: K8s ConfigMap/CRD 변경을 KV에 반영 (REST API)
- Pod: KV에서 현재값 스냅샷 + 변경 이벤트 Watch

시작 동작
- Distributor는 시작 시 Kubernetes informer cache를 동기화한 뒤 현재 최종 상태로 KV를 reconcile하고, 이후 변경은 watch로 계속 반영

메트릭
- `/metrics`에서 이벤트 처리 성공률과 의존성 상태 메트릭을 Prometheus 텍스트 포맷으로 노출

요구 사항
- JetStream 활성화된 NATS 서버

라벨 필터 (동기화 조건)
- `config.upm.io/managed: "true"` 라벨이 붙은 ConfigMap/Secret만 NATS(KV)에 동기화
- 환경변수로 라벨 키/값을 변경 가능
  - `MANAGED_LABEL_KEY` (기본: `config.upm.io/managed`)
  - `MANAGED_LABEL_VALUE` (기본: `true`)

실행 예시
```bash
# NATS 서버 (예시)
# nats-server -js

# Pod (watcher) 실행
cd jetstream_kv_config/pod
NATS_URL=nats://localhost:4222 go run .

# Distributor 실행 (REST API)
cd ../cmd/distributor
PORT=8080 NATS_URL=nats://localhost:4222 \
WATCH_NAMESPACES=default \
MANAGED_LABEL_KEY=config.upm.io/managed \
MANAGED_LABEL_VALUE=true \
go run .

# 예시 요청
curl -X PUT http://localhost:8080/namespaces/default/configmap/appA \\
  -H 'Content-Type: text/plain' \\
  --data-binary $'app: appA\\nversion: 1\\n'

curl http://localhost:8080/namespaces/default/configmap/appA
curl http://localhost:8080/namespaces/default/configmap
```

핵심
- 신규 Pod도 KV에서 스냅샷을 바로 수신
- 이후 변경은 `Watch`로 계속 수신
