# Ticker Monitor

GitHub Actions 기반 미국 주식 모니터링 봇입니다. 기본값은 `$HOOD`지만, `monitor_config.md`에서 종목을 바꾸면 같은 기능을 다른 티커에 적용할 수 있도록 재개발 중입니다.

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
cik: 0001674101
benchmark: $QQQ
peer_tickers: $ETN, $PWR, $SMCI
app_store_id:
market_scan_focus: $VRT
```

`cik`는 SEC Form 4 내부자 거래 조회에 필요합니다. 해당 종목에 앱스토어 순위 추적이 없으면 `app_store_id`는 비워두면 됩니다. `MONITOR_PROFILE` 또는 `MONITOR_TICKER` 환경 변수로도 실행 시점에 종목을 바꿀 수 있습니다.

## 상태 파일

런타임 상태와 캐시는 종목별 디렉터리에 저장됩니다.

```text
state/HOOD/state.json
state/HOOD/weekly_state.json
state/HOOD/beta_cache.json
state/HOOD/app_rank_cache.json
```

`ticker: $VRT`로 바꾸면 같은 이름의 파일들이 `state/VRT/` 아래에 새로 생성됩니다.

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

## 수동 실행

```bash
pip install -r requirements.txt

python hood_monitor.py normal
python hood_monitor.py close
python hood_monitor.py morning
python hood_monitor.py weekly
python hood_monitor.py 13f

python market_scan.py
python market_scan.py --ticker VRT

python backtest.py --ticker VRT --years 2 --no-slack
```

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

## Slack 알림 품질

모니터 알림 상단에는 `긴급/주의/참고` 요약이 먼저 표시됩니다. 요약은 주가 급변, 기술지표, 옵션/공매도, SEC 내부자 거래, 뉴스, DCA 점수를 기준으로 핵심 이유를 최대 4개까지 보여줍니다.

## 다음 재개발 후보

- `hood_monitor.py`를 데이터 수집, 점수 계산, Slack 출력, 상태 관리 모듈로 분리
- 상태 파일을 개인 포지션 정보와 알림 중복 방지 정보로 분리
- SEC/Yahoo/FINRA 파서 단위 테스트 추가
- 여름/겨울 미국장 시간대를 명시적으로 처리
- S&P 500 종목 목록을 코드가 아니라 데이터 파일로 분리
