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
- Stage 2 시장 경로는 XNYS 휴장·조기폐장 캘린더, Yahoo 5분봉, 인증 multi-quote, 고정 전일 종가, SOXX/피어 상대 흐름, 동시간대 20세션 거래량과 intraday replay를 구현했습니다. 새 거래일의 첫 GitHub 실행이 늦어져도 당일 04:00 ET 이후 봉을 처음부터 복원해 오전 급변 후 되돌림을 놓치지 않습니다. 10분을 넘겨 복구된 MOVE/VOLUME은 실시간 알림처럼 보이지 않도록 실제 발생 시각과 복구 지연을 `지연 감지`로 표시합니다.
- Stage 3 근거 경로는 Yahoo Search와 발행사 IR RSS, 결정론적 저가치 필터, 사건 클러스터링, 원문 근거 검증 AI 분석, SEC 403 Yahoo 미러 복구, 신규 공시 기준선과 재시도 ledger를 구현했습니다. 뉴스 제목에 회사를 직접 명시하지 않은 업종 일반론과 목표가·주가 해설·펀드 보유 기사는 AI 전에 제외합니다. 검증된 사실과 거래 금액까지 비교해 7일 안의 재전재를 최초 canonical 사건에 연결하고, 최근 촉매는 사건당 한 건만 노출합니다. 같은 AI 응답에서 사건 유형·회사 직접성·새 사실·중요도·출처 계층을 구조화하고, 고신뢰·고중요도의 인수·실적/가이던스·대형 계약/고객·경영진·규제 사건만 독립 알림으로 허용합니다. 생산능력·제품·파트너십·자금조달은 브리핑 전용, 불확실하거나 논평성인 자료는 ledger 전용입니다.
- Stage 4 브리핑은 실제 XNYS 마감 15분 후 일일 브리프와 월요일 08:13 KST 주간 리뷰를 생성합니다. 일일 브리프는 방향, SOXX, 피어 평균, 20거래일 거래량, 당일 핵심 근거 순서를 고정하고, 주간 리뷰는 완료된 미국 거래 주간의 상대 흐름과 high-confidence 강화·위험 근거만 요약합니다. 공식 IR 원문에서 날짜가 검증된 다음 주 일정만 링크와 함께 표시합니다.
- 모든 v2 사용자 메시지는 alert/outbox 저장 전에 길이, 필수 정보, 금지 문구, 원문 링크와 중복 블록 품질 게이트를 통과해야 합니다. `quality-report`는 최근 메시지 판정, `schedule`/`push`/수동 실행 수, Actions run 생성부터 tick 시작까지의 지연, 같은 장중 schedule 사이의 실제 간격, task별 실행 시간, Yahoo market/news, IR, SEC, AI pipeline의 성공·복구·실패와 latency, outbox/evidence 상태를 Actions Summary에 기록합니다. 개발 push 간격을 scheduler 건강도로 계산하지 않습니다. 저장된 근거를 현재 필터·canonical 사건과 다시 대조해 소급 저가치 및 재전재 중복 알림도 별도 판정합니다. schema v8부터 run과 alert에 build SHA, workflow, run ID, 실제 DB 기록 시각을 보존하고, schema v9부터 시장 관측에도 동일 provenance를 남겨 과거 `legacy` 결과와 현재 배포 버전의 품질을 분리합니다.
- `replay-market`은 운영 DB와 Slack을 사용하지 않는 격리 DB에서 완료된 거래일의 Yahoo 5분봉을 재생합니다. 동일 방향 최고 구간 압축, 양·음 방향 반전, 재실행 중복 0건, 상대 흐름·실제 baseline 거래량과 메시지 품질을 검증합니다.
- 품질 리포트의 Stage 5 판정은 GitHub `schedule` poll coverage와 Yahoo 5분봉 복구 coverage를 분리합니다. 스케줄 지연은 감추지 않고 advisory로 남기되, XNYS 조기폐장을 포함한 세션별 5분 구간을 모두 복원했는지로 전체 거래일을 계산합니다. 출시 게이트는 현재 build SHA가 직접 만든 schedule/provider 기록, 메시지, 시장 관측, 근거 알림만 사용하며 이전 빌드의 정상 이력을 승계하지 않습니다. 현재 빌드의 완전한 거래일 2개, 거래일당 비정기 알림 0~3건, 저가치·중복 근거 0건을 채우기 전에는 `blocked` 또는 `observing`이며 production 전환을 허용하지 않습니다.
- production 전달 코어는 Slack 호출 전에 outbox를 `sending`으로 원격 checkpoint하고, 호출 결과를 `delivered`, `failed`, `discarded`, `delivery_unknown`으로 구분해 다시 checkpoint할 수 있습니다. timeout처럼 수락 여부가 모호한 요청은 자동 재발송하지 않습니다. 이 경로는 Stage 5 관측 완료 전 workflow에서 활성화하지 않습니다.
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
investing-monitor quality-report
investing-monitor replay-market --days 3
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
