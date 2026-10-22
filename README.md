# DAPAC
"DAPAC: Deadline-Aware Prefetch-Aided Caching with Online Content and Request-Time Prediction," submitted to International Conference on Information Networking (ICOIN), Nha Trang, Vietnam, 2027

## Implementation snapshot

현재 운영 중인 KOREN 온타임 캐싱 PoC의 알고리즘 코드 스냅샷입니다. `ack-committed-v3`의 **ACK 검증 → 사용자별·영상별 재생 위치 갱신 → 다음 요청 예측 → 선반입 예약** 순서를 유지합니다.

## 구성

```text
Campus content origin → KOREN intermediate cache → KOREN synthetic-user receiver
```

- `cache/poc.py`: 위치·현재 영상 조건 마르코프 모델, 사용자별 요청 간격 EMA, LRU/TTL 캐시, 다운로드 작업자.
- `cache/experiment.py`: 캐싱 없음 / 요청 기반 LRU / 영상 인기도 Top-1 선반입 / 온타임을 1시간씩 순환.
- `cache/cache_api.py`: 실제 세그먼트 생성·HTTPS 응답, 가상 사용자와 영상별 이어보기, 수신 해시 검증·중복 ACK 처리.
- `cache/cache_transport.py`: mTLS 원본 다운로드와 수신자 전달.
- `cache/image_https.py`, `cache/relay.py`: 실행본이 의존하는 HTTPS·이미지 교환 지원 모듈. 캐싱과 공유하는 원래 의존성을 유지합니다.
- `cache/experiment_report.py`, `cache/experiment-status.py`: 실험 기록 내보내기 및 신선한 지표 확인.
- `cache/ontime-cache-route.py`: 예시 주소에 대한 source-policy routing 검사·복구. 로컬 시스템 경로를 바꾸므로 설정을 검토한 후 사용하세요.
- `tools/generate-tls.sh`: 자체 테스트/배포를 위한 새 인증서 생성. 운영 중인 키와 인증서는 포함하지 않습니다.

## 현재 실험 프로필

| 항목 | 값 |
|---|---|
| 가상 사용자 / 영상 스트림 | 8 / 8 |
| 세그먼트 크기 | 2 MiB |
| 공유 캐시 / TTL | 128 MiB / 30초 |
| 다음 영상 모델 | 사용자 전체 이력 공유, 현재 위치·영상으로 조건화한 Markov |
| 다음 요청 시점 | 사용자별 EMA, 계수 0.2 |
| 원본 다운로드 시간 | 공유 EMA, 계수 0.2; 요청 시작부터 payload SHA-256 검증까지 |
| 신뢰도 임계값 | 0.55 |
| Safety Margin | 운영 프로필 1초; 기본 CLI 값과 기존 기준선은 0.5초 |
| 원본 페이로드 속도 상한 | 100 Mbps |
| 구성한 요청 부하 | 합계 110 Mbps 목표; 실제 달성값은 다름 |

영상은 실제 디코딩 영상 파일이 아닌 결정적 합성 세그먼트입니다. 영상별 총 길이/종료는 없으며, 처음 방문은 0부터, 재방문은 사용자별 해당 영상의 다음 세그먼트부터 이어집니다. 세그먼트 key `(stream, seq)`는 모든 사용자가 공유합니다.

## 테스트

Linux, Python 3.10 이상, Bash, OpenSSL이 필요합니다. Python 패키지 추가 설치는 필요하지 않습니다. 통합 테스트는 loopback에서 임시 mTLS 서버와 인증서를 생성해 실제 바이트 전송을 검증합니다.

```bash
bash tools/check.sh
```

테스트는 운영 PC에 연결하거나 systemd/방화벽/실제 라우팅을 수정하지 않습니다. route 테스트는 계획 함수만 검증합니다.

## 실행

이 저장소의 주소는 실제 배포 IP가 아닌 **문서용 예시 주소**입니다.

| 역할 | 예시 주소 |
|---|---|
| Origin | `198.51.100.10:443` |
| Cache / source bind | `192.0.2.20` |
| User receiver | `192.0.2.21:8443` |

실제 구성에 맞춰 CLI/TLS 경로, 서버 JSON, 소스 주소 검증 상수, 라우팅 helper의 주소·인터페이스를 조정하세요. 이 스냅샷은 원본의 고정 배포 기본값 구조를 유지하므로 JSON 하나만 바꿔 모든 검증 상수가 바뀌지는 않습니다. 실행 중인 KOREN 배포에는 이 예시 파일들을 덮어쓰지 마세요.

mTLS origin과 receiver가 준비된 상태에서, 필요한 주소·인증서 경로를 명시하는 명령은 `config/controller.example.sh`를 참고하세요. 원격 사용자 서비스의 workload가 컨트롤러 설정과 일치해야 합니다. 실험 controller는 receiver의 `/client/reset`을 호출하므로 운영 수신기의 사용자 상태를 초기화합니다.

```bash
python3 cache/experiment.py run --help
python3 cache/cache_api.py --config /path/to/clients.json
python3 cache/experiment_report.py --state /path/to/state --experiment-id margin-1s
```

`systemd/ontime-cache-experiment.service.example`은 서비스 구성 예시이며 자동 설치하지 않습니다. 기존 실험은 0.5초와 1초를 별도 DB로 보관합니다. 같은 experiment ID에 다른 Margin을 넣어 기록을 섞는 것은 거부합니다.

## 해석 시 주의할 구현 세부사항

- 요청 처리는 사용자 전체에 대해 직렬이고, 다운로드 작업자 1개가 별도로 동작합니다.
- 캐시에 완성된 세그먼트가 있어야 hit입니다. 예약 대기·다운로드 중은 miss입니다.
- `prefetch_late_requests`는 대기 중/진행 중인 선반입을 합친 값이며, 실제 다운로드 중만 분리한 비율이 아닙니다.
- stale speculative job 취소는 `Fetcher.request()` 시작 시 수행하므로 demand 처리 전과 ACK 후 새 prefetch 요청 시 모두 실행될 수 있습니다.
- `Cache.get()`은 만료 항목을 회수하지만, 현재 실행 스냅샷의 `Cache.put()`에는 용량 검사 전 `expire()` 호출이 없습니다. 이 점을 임의로 수정해 기존 결과와 섞지 않았습니다.
- ACK 실패는 해당 요청의 예측 업데이트를 건너뛰고 구간을 오류 처리합니다. 종료 정리 중 진행 중인 전송과 최종 지표 저장은 완료될 수 있습니다.
- 총 원본 전송량은 처리 요청 수에 영향을 받습니다. 비교 시 요청당 원본 바이트와 지연을 함께 보세요.
- `.53`과 `.52`에 해당하는 두 KOREN 호스트의 로컬 전송량은 백본 링크 사용량 측정과 다릅니다.

## 포함하지 않은 파일

운영 인증서·개인 키·SSH 설정·실험 DB·원본 CSV·로그·사용자 이미지 데이터는 포함하지 않습니다. 원본 IP와 개인 경로는 예시 값으로 치환했습니다. 원본/업로드본 해시와 변환 내역은 `SOURCE_MANIFEST.json`에 있습니다. 원고/실험 성능표는 이 코드 저장소의 범위에 포함하지 않았습니다.

저장소의 기존 논문 소개를 유지하고 현재 실행 알고리즘의 코드 스냅샷을 추가했습니다. 라이선스 파일은 임의로 추가하지 않았습니다.
