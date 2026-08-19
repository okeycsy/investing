# Ticker Thesis Monitor

GitHub Actions 기반 단일 종목 투자 논지 모니터입니다. 현재 기본 종목은 `$VRT`입니다. 가격 화면이 아니라 중요 뉴스, 발행사 SEC 공시, 내부자 거래, 장마감 상대 성과와 주간 논지 변화를 Slack으로 전달합니다.

제품 동작과 표시 금지사항은 `HOOD_MONITOR_PRODUCT.md`에 고정되어 있습니다.

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

`SEC_CONTACT`는 SEC 요청의 `From` 헤더에 들어갈 연락처입니다. SEC가 특정 이메일 도메인을 `User-Agent` 안에서 차단할 수 있어 연락처는 별도 헤더로 보냅니다. 필요하면 `SEC_USER_AGENT`, legacy EDGAR/Archives 경로는 `SEC_LEGACY_USER_AGENT` 환경 변수로 앱 식별자를 직접 지정할 수 있습니다.

## 수동 실행

```bash
pip install -r requirements.txt

python hood_monitor.py normal
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

- 장중 급등락은 `±4%` 정수 구간부터 시작해 같은 방향의 새 1%p 구간 진입 때만 전송합니다. 당일 반대 방향 전환과 이미 통과한 구간은 다시 알리지 않습니다.
- 장중에는 새 뉴스, 분석 완료된 발행사 중요 SEC 공시, 내부자 거래, 큰 폭의 이상 움직임, 20거래일 평균 대비 1.5배 이상 거래량만 전송합니다. 거래량 알림은 하루 한 번만 보냅니다.
- 상대 흐름은 반도체 지수 `SOXX`와 동일가중 피어 평균 `ETN/NVT/GEV`를 함께 사용합니다.
- 장마감에는 `양전/음전/보합`, 지수·피어 상대 성과, 투자 논지와 DCA 계획을 먼저 표시하고 당일 거래량과 20일 평균을 비교합니다.
- 10-Q/10-K는 SEC XBRL 핵심 수치를 전년 동기와 비교해 요약합니다. 같은 실적 사건의 8-K는 중복 알림에서 제외합니다.
- 주간 브리핑은 실제 5거래일 변화와 최신 거래량 비교를 사용합니다.
- AI 분석 실패 뉴스는 확인 완료로 저장하지 않으며 다음 실행에서 재시도합니다.
- 기술지표는 DCA 보조 맥락이며 자동 매수 지시가 아닙니다.
