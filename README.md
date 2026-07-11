# Ticker Monitor

GitHub Actions 기반 미국 주식 모니터링 봇입니다. 현재 기본 모니터링 종목은 `$VRT`이며, `monitor_config.md`에서 종목을 바꾸면 같은 기능을 다른 티커에 적용할 수 있습니다.

## 1차 정리 범위

- `monitor_config.md`에서 기본 종목을 읽고, `profiles/{TICKER}.md`가 있으면 회사명, CIK, 벤치마크, 피어 종목을 자동 적용합니다.
- `hood_monitor.py`, `market_scan.py`, `backtest.py`의 기본 종목 설정을 공통화했습니다.
- `requirements.txt`를 실제 실행 의존성과 맞췄습니다.
- `gitignore`를 `.gitignore`로 바로잡았습니다.
- GitHub Actions 설치 명령을 `pip install -r requirements.txt`로 통일했습니다.
- `Live Smoke Test`는 `main` push 때도 Yahoo/SEC/Slack 실전 연결을 확인합니다.
- 상태와 캐시는 `state/{TICKER}/` 아래에 종목별로 저장합니다.

## 종목 변경

`monitor_config.md`를 수정합니다.

```text
ticker: $VRT
```

`profiles/VRT.md`가 있으면 회사명, CIK, 벤치마크, 피어 종목이 자동으로 붙습니다. 새 종목은 `profiles/{TICKER}.md`를 추가하면 됩니다.

```text
ticker: $VRT
company_name: Vertiv Holdings Co
company_aliases: Vertiv, Vertiv Holdings
exchange: NYSE
country: United States
currency: USD
sector: Industrials
industry: Critical Digital Infrastructure, Power & Thermal Management
cik: 0001674101
benchmark: $QQQ
peer_tickers: $ETN, $NVT, $PWR, $SMCI
end_markets: data centers, communication networks, commercial and industrial environments
core_products: critical power, UPS systems, power distribution, thermal management, liquid cooling
news_keywords: Vertiv, critical digital infrastructure, data center power, data center cooling, thermal management
watch_themes: AI data center capex, hyperscaler buildout, power grid demand
priority_keywords: earnings, guidance, backlog, organic orders, AI data center, liquid cooling, Nvidia
risk_keywords: order slowdown, margin pressure, supply chain, tariffs, customer concentration, competition
profile_source_url: https://investors.vertiv.com/overview/default.aspx
app_store_id:
market_scan_focus: $VRT
```

`cik`는 SEC Form 4 내부자 거래 조회에 필요합니다. `company_aliases`, `core_products`, `news_keywords`, `watch_themes`, `priority_keywords`, `risk_keywords`는 뉴스 후보를 놓치지 않기 위한 감시 키워드입니다. 해당 종목에 앱스토어 순위 추적이 없으면 `app_store_id`는 비워두면 됩니다. `MONITOR_PROFILE` 또는 `MONITOR_TICKER` 환경 변수로도 실행 시점에 종목을 바꿀 수 있습니다.

## 상태 파일

런타임 상태와 캐시는 종목별 디렉터리에 저장됩니다.

```text
state/VRT/state.json
state/VRT/weekly_state.json
state/VRT/beta_cache.json
state/VRT/app_rank_cache.json
```

현재 기본값인 `ticker: $VRT`는 같은 이름의 파일들을 `state/VRT/` 아래에 생성합니다. 기존 HOOD 기록은 `state/HOOD/`에 남아 있습니다.

## 코드 구조

- `hood_monitor.py`: 실행 모드 조립, Yahoo/FINRA/SEC 호출 흐름, 알림 발송 오케스트레이션
- `monitor_models.py`: 가격, 기술지표, SEC 거래, DCA 점수 데이터 구조
- `monitor_state.py`: 런타임 상태 파일 로드/저장
- `sec_filings.py`: SEC Form 4 / 13F XML 파싱
- `alert_quality.py`: Slack 알림 상단의 긴급/주의/참고 요약 판단
- `slack_blocks.py`: Slack Block Kit 공통 블록과 전송 유틸

## GitHub Secrets

필수:

- `SLACK_WEBHOOK_URL`

선택:

- `MARKET_SCAN_WEBHOOK`
- `ANTHROPIC_API_KEY`
- `IMGUR_CLIENT_ID`
- `SEC_CONTACT`
- `SEC_USER_AGENT`
- `SEC_LEGACY_USER_AGENT`

`SEC_CONTACT`는 SEC 요청의 `From` 헤더에 들어갈 연락처입니다. SEC가 특정 이메일 도메인을 `User-Agent` 안에서 차단할 수 있어 연락처는 별도 헤더로 보냅니다. 필요하면 `SEC_USER_AGENT`, legacy EDGAR/Archives 경로는 `SEC_LEGACY_USER_AGENT` 환경 변수로 앱 식별자를 직접 지정할 수 있습니다.

## 알림 종류와 스케줄

시장 관련 알림은 GitHub Actions가 넓은 시간대에 주기적으로 깨어난 뒤, `market_calendar.py`가 NYSE 달력 기준으로 실제 실행 여부를 결정합니다. 서머타임/겨울시간, 휴장일, 조기마감일은 `pandas_market_calendars`의 NYSE 캘린더를 사용합니다.

| 알림 | workflow | 실행 시간 |
| --- | --- | --- |
| VRT 장중 모니터 | `VRT Monitor` / `normal` | NYSE pre/regular/post-market 중 매시 1회 |
| VRT 장마감 브리핑 | `VRT Monitor` / `close` | NYSE 공식 마감 + 60분 |
| VRT 아침 재확인 | `VRT Monitor` / `morning` | NYSE 공식 마감 + 90분 |
| VRT 주간 브리핑 | `VRT Monitor` / `weekly` | UTC 일 23:00 = KST/JST 월 08:00 |
| VRT 13F 기관 포지션 | `VRT Monitor` / `13f` | UTC 토 10:00 = KST/JST 토 19:00 |
| VRT 단일 종목 스캔 | `VRT Market Scan` | NYSE 공식 마감 + 120분 |
| VRT 수동 실행 컨트롤 | `VRT Alert Control` | 수동 실행 |
| VRT 라이브 연결 점검 | `Live Smoke Test` | `main` push 또는 수동 실행 |
| VRT 실제 시작 점검 | `VRT Startup Digest` | 수동 실행, 관련 파일 변경 push |
| VRT 알림 발송 샘플 점검 | `VRT Sample Alert Delivery Smoke` | 수동 실행 |
| VRT 백테스트 | `V3 Score Backtester` | 수동 실행 |
| VRT DCA 현황/업데이트 | `VRT Monitor` / `dca_status`, `dca_update` | 수동 실행 |

## 수동 실행

```bash
pip install -r requirements.txt

python hood_monitor.py normal
python hood_monitor.py close
python hood_monitor.py morning
python hood_monitor.py weekly
python hood_monitor.py 13f

python market_scan.py --ticker VRT

python backtest.py --ticker VRT --years 2 --no-slack
python startup_digest.py --no-slack
```

GitHub에서는 Actions 탭의 `VRT Alert Control`을 누르면 실제 시작 점검, 실제 장중/마감/주간/13F 알림, 실제 단일 종목 스캔, 라이브 연결 점검, 샘플 알림 점검을 한 화면에서 선택할 수 있습니다. 이름이 `actual_`로 시작하는 항목은 Yahoo/SEC/뉴스/Market Scan을 실제 조회해서 Slack으로 보냅니다. `sample_delivery_smoke`는 발송 형식만 확인하는 샘플입니다.

예약 실행은 `schedule_state.json`에 dispatch key를 남깁니다. GitHub Actions 지연이나 재시도로 같은 `close:{거래일}` 또는 `market_scan:{거래일}` 키가 다시 들어오면 중복 Slack 발송을 건너뜁니다.

## 라이브 점검

Yahoo/SEC/Slack이 실제로 연결되는지 확인하려면:

```bash
python live_smoke.py --require-slack
```

Slack Webhook 없이 Yahoo/SEC만 확인하려면:

```bash
python live_smoke.py --no-slack
```

GitHub에서는 Actions 탭의 `Live Smoke Test`를 수동 실행하면 됩니다. `main`에 push될 때도 자동 실행되며, 이때는 Slack Secret까지 필수로 확인합니다. `workflow_dispatch`에서 `require_slack=false`를 고르면 Slack 없이 Yahoo/SEC만 확인할 수 있습니다.

## 자동 테스트

코드, 프로필, 테스트, 의존성이 바뀌면 GitHub Actions의 `Tests`가 자동으로 실행됩니다. 로컬에서는 아래 명령으로 같은 검사를 돌릴 수 있습니다.

```bash
python -m py_compile monitor_config.py monitor_models.py monitor_state.py slack_blocks.py sec_filings.py alert_quality.py market_calendar.py schedule_state.py hood_monitor.py startup_digest.py market_scan.py alert_delivery_smoke.py live_smoke.py
python -m unittest discover tests
```

## Slack 알림 품질

모니터 알림 상단에는 `긴급/주의/참고` 요약이 먼저 표시됩니다. 요약은 주가 급변, 기술지표, 옵션/공매도, SEC 내부자 거래, 뉴스, DCA 점수를 기준으로 핵심 이유를 최대 4개까지 보여줍니다.

뉴스는 `관련 확정 뉴스`와 `{티커} 확인 후보 뉴스`를 구분합니다. Claude가 직접 영향 뉴스로 판단한 기사는 요약/번역이 표시되고, 직접 영향 필터를 통과하지 않았더라도 설정 종목 감시 키워드가 잡힌 기사는 후보로 따로 표시합니다. `priority_keywords` 또는 `risk_keywords`가 잡힌 후보는 `우선/리스크` 라벨과 함께 표시됩니다.

## 다음 재개발 후보

- Yahoo/가격 수집과 기술지표/DCA 점수 계산 추가 분리
- 상태 파일을 개인 포지션 정보와 알림 중복 방지 정보로 분리
- Yahoo/FINRA 파서 단위 테스트 추가
- S&P 500 종목 목록을 코드가 아니라 데이터 파일로 분리
