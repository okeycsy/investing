# Ticker Thesis Monitor

> `hood_monitor`는 현재 안정성 중심으로 재개발 중입니다. 제품 진단, 알림 계약,
> 목표 아키텍처와 단계별 전환 기준은
> [HOOD Monitor 재개발 기준서](docs/HOOD_MONITOR_REBUILD.md)를 따릅니다.
> 세부 기능, polling 주기, 복구 정책과 수용 기준은
> [HOOD Monitor PRD v2](docs/HOOD_MONITOR_PRD_V2.md)에 고정합니다.
> `src/investing_monitor`는 기존 운영 경로와 분리된 새 코어이며 아직 production
> Slack을 대신하지 않습니다.

단일 종목 투자 논지 모니터입니다. 현재 기본 종목은 `$VRT`입니다. 새 버전도 GitHub Actions hosted runner에서 실행하며 Yahoo REST를 장중 5분 schedule로 조회합니다. 예약 지연이나 누락은 다음 성공 실행의 intraday replay로 보완하고, 중요 뉴스, 발행사 SEC 공시, 내부자 거래, 장마감 상대 성과와 주간 논지 변화를 Slack으로 전달합니다.

제품 동작과 표시 금지사항은 `HOOD_MONITOR_PRODUCT.md`에 고정되어 있습니다.

## v2 개발 상태

- Stage 0 계약은 문서와 기존 제품 테스트로 동결되어 있습니다.
- Stage 1 기반은 versioned SQLite, due-task planner, run/task checkpoint, outbox 상태 전이, `runtime-state` rolling snapshot으로 구현되어 있습니다.
- Stage 2 시장 경로는 XNYS 휴장·조기폐장 캘린더, Yahoo 5분봉, 인증 multi-quote, 고정 전일 종가, SOXX/피어 상대 흐름, 동시간대 20세션 거래량과 intraday replay를 구현했습니다.
- Stage 3 근거 경로는 Yahoo Search와 발행사 IR RSS, 결정론적 저가치 필터, 사건 클러스터링, 원문 근거 검증 AI 분석, SEC 403 Yahoo 미러 복구, 신규 공시 기준선과 재시도 ledger를 구현했습니다.
- Stage 4의 일일 장마감 브리프는 실제 XNYS 마감 15분 후 한 번만 생성하며, 방향, SOXX, 피어 평균, 20거래일 거래량, 당일 핵심 근거 순서를 고정했습니다. 스케줄 누락 시 다음 거래일 장전까지 복구하며 weekly 브리프는 다음 개발 대상입니다.
- SEC 인라인 XBRL의 숨김 메타데이터는 제거하고 10-Q/10-K의 실제 MD&A를 우선 추출합니다. 폼 번호만 있거나 본문을 확보하지 못한 공시는 알림으로 만들지 않습니다.
- Form 4와 Yahoo 내부자 집계는 거래 코드를 구조화해 `P` 장내매수와 `S` 장내매도만 materiality 기준을 통과할 수 있습니다. `A` 보상, `M` 옵션 행사, `F` 세금 처리는 독립 알림 없이 ledger에만 저장합니다.
- `Monitor V2 Runtime Shadow`는 관련 코드가 `main`에 push될 때 테스트와 계획 출력을 자동 검증하며, 수동 실행에서는 상태 진단과 snapshot 저장을 선택할 수 있습니다.
- 동일 workflow는 DST 양쪽을 포괄하는 UTC 창에서 정각을 피한 5분 schedule로 market/news/SEC/close shadow tick을 실행하고 `runtime-state`를 checkpoint합니다. 예약 실행에서는 운영 의존성만 설치하며 전체 회귀 테스트는 push와 수동 실행에서 수행합니다. 개발 push도 Slack 없이 통합 shadow와 checkpoint를 한 번 실행해 provider·secret 회귀를 즉시 드러냅니다.
- 20회 연속 checkpoint에서 `main` 비침범, 새 runner 복원, stale runner 충돌 차단을 테스트합니다.
- production Slack은 아직 legacy `hood_monitor.py` 경로가 담당합니다. Stage 4~5 검증 전에는 v2로 전환하지 않습니다.

로컬에서 Stage 1 상태를 확인하려면:

```bash
pip install -e .
investing-monitor plan
investing-monitor status
investing-monitor doctor
investing-monitor market-tick --config monitor_config.md
investing-monitor shadow-tick --config monitor_config.md
```

## 1차 정리 범위

- `monitor_config.md`에서 기본 종목, 회사명, CIK, 벤치마크, 피어 종목을 읽습니다.
- `hood_monitor.py`, `market_scan.py`, `backtest.py`의 기본 종목 설정을 공통화했습니다.
- `requirements.txt`를 실제 실행 의존성과 맞췄습니다.
- `gitignore`를 `.gitignore`로 바로잡았습니다.
- GitHub Actions 설치 명령을 `pip install -r requirements.txt`로 통일했습니다.

## 종목 변경

`monitor_config.md`를 수정합니다.

```text
ticker: $VRT
company_name: Vertiv Holdings Co
cik: 0001674101
benchmark: $SOXX
peer_tickers: $ETN, $NVT, $GEV
app_store_id:
market_scan_focus: $VRT
```

`cik`는 SEC Form 4 내부자 거래 조회에 필요합니다. 해당 종목에 앱스토어 순위 추적이 없으면 `app_store_id`는 비워두면 됩니다.

## GitHub Secrets

필수:

- `SLACK_WEBHOOK_URL`
- `ANTHROPIC_API_KEY`

선택:

- `MARKET_SCAN_WEBHOOK`
- `IMGUR_CLIENT_ID`
- `SEC_CONTACT`
- `SEC_USER_AGENT`
- `SEC_LEGACY_USER_AGENT`
- `SEC_API_KEY` (GitHub-hosted runner에서 13F 중계 조회 시 사용)
- `SEC_DATA_MODE` (`auto`, `direct`, `yahoo`)
- `SEC_ARCHIVE_MODE` (`auto`, `direct`, `yahoo`)

`SEC_CONTACT`는 SEC 공식 지침에 맞춰 `User-Agent`와 `From` 헤더에 함께 들어갑니다. GitHub-hosted runner의 공유 IP는 `data.sec.gov`, `efts.sec.gov`, raw Archives에서 간헐적으로 차단되므로 운영 워크플로는 `SEC_DATA_MODE=yahoo`, `SEC_ARCHIVE_MODE=yahoo`를 사용합니다. 발행사 공시·재무제표·Form 4는 Yahoo 대체 경로로 조회하고, 13F는 `SEC_API_KEY`가 있을 때만 sec-api.io 중계 경로를 사용합니다.

## 수동 실행

```bash
pip install -r requirements.txt

python hood_monitor.py normal
python hood_monitor.py realtime
python hood_monitor.py close
python hood_monitor.py weekly
python hood_monitor.py 13f

python market_scan.py
python market_scan.py --ticker VRT

python backtest.py --ticker VRT --years 2 --no-slack
```

## 라이브 점검

Yahoo/SEC/AI/Slack이 실제로 연결되는지 확인하려면:

```bash
python live_smoke.py --require-slack
```

Slack Webhook 없이 Yahoo/SEC/AI만 확인하려면:

```bash
python live_smoke.py --no-slack
```

GitHub에서는 Actions 탭의 `Live Smoke Test`를 수동 실행하면 됩니다. `require_slack=true`일 때는 `SLACK_WEBHOOK_URL` 또는 `MARKET_SCAN_WEBHOOK` Secret이 없으면 실패 처리됩니다.

내부자 거래는 SEC 제출 목록과 Form 4 원문을 우선 조회합니다. SEC Archives가 자동화 요청을 `403`으로 제한하면 Yahoo 내부자 거래 데이터로 자동 전환합니다.

## 알림 원칙

- 현재 legacy workflow는 가격을 10분 cron, 뉴스·공시를 시간 단위 cron으로 조회합니다. v2 cutover 후에는 GitHub Actions의 정각을 피한 5분 tick으로 통합하고, 지연된 close/news/SEC와 누락된 가격 구간은 다음 성공 tick에서 복원합니다.
- 장중 급등락은 `±4%` 정수 구간부터 시작해 같은 방향의 새 1%p 구간 진입 때만 전송합니다. 당일 반대 방향 전환과 이미 통과한 구간은 다시 알리지 않습니다.
- 장중에는 새 뉴스, 분석 완료된 발행사 중요 SEC 공시, 내부자 거래, 큰 폭의 이상 움직임, 20거래일 평균 대비 1.5배 이상 거래량만 전송합니다. 거래량 알림은 하루 한 번만 보냅니다.
- 상대 흐름은 반도체 지수 `SOXX`와 동일가중 피어 평균 `ETN/NVT/GEV`를 함께 사용합니다.
- 방향은 `📈/📉`, 지수·피어 상대 흐름은 `↗️/↘️/↔️`만 사용해 핵심 신호를 빠르게 구분합니다.
- 장마감에는 `양전/음전/보합`, 지수·피어 상대 성과, 투자 논지를 먼저 표시하고 당일 거래량과 20일 평균을 비교합니다.
- 투자 논지의 훼손·위험·강화 판단은 제목과 확인된 근거를 함께 표시할 수 있을 때만 사용합니다. 목표주가·고평가/저평가 의견, 종목 비교, 로펌의 주주 모집성 조사 홍보는 논지 변화에서 제외합니다.
- 10-Q/10-K는 SEC XBRL 핵심 수치를 전년 동기와 비교해 요약합니다. 같은 실적 사건의 8-K는 중복 알림에서 제외합니다.
- 발행사 SEC 공시는 종목별 전용 `*_sec_alert_cache.json`에 처리 해시를 저장해 최초 한 번만 알립니다. 새 캐시 도입 시 기존 공시는 기준선으로만 등록합니다.
- 주간 브리핑은 실제 5거래일 변화와 최신 거래량 비교를 사용합니다.
- AI 분석 실패 뉴스는 확인 완료로 저장하지 않으며 다음 실행에서 재시도합니다.
- 기술지표와 수급 데이터는 관측값만 전달하며 매수 계획이나 점수로 변환하지 않습니다.
