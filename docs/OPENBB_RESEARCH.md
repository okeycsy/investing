# 🔬 OpenBB 리서치 & $BE Insight Monitor

`research.py`(원커맨드 리서치 노트)와 `be_monitor.py`(집중 포지션용 Slack 봇)는 [OpenBB Platform](https://github.com/OpenBB-finance/OpenBB)
위에서 동작합니다. 데이터 소스는 전부 무료(yfinance / SEC EDGAR·XBRL / FINRA)이고 API 키가 필요 없습니다.

## 설치 (로컬)

```bash
python3.12 -m venv .venv && .venv/bin/pip install -r requirements-openbb.txt && .venv/bin/pip install --no-deps openbb
```

> Python 3.10~3.12 권장. pyenv로 빌드한 Python은 `_lzma` 모듈이 없어 OpenBB가 import되지 않을 수 있습니다(Homebrew Python 사용).
> 첫 import 때 OpenBB가 정적 자산을 빌드하느라 15초 정도 걸립니다.

## research.py — 티커 하나로 리서치 노트

```bash
.venv/bin/python research.py BE                       # research/BE_<date>.md 생성 (be_config.json의 peers 사용)
.venv/bin/python research.py NVDA --peers AMD,AVGO    # 임의 종목 + 동종업체
.venv/bin/python research.py BE --ai                  # 10-Q/10-K Claude 요약 추가 (ANTHROPIC_API_KEY)
.venv/bin/python research.py BE --no-sec              # SEC 섹션 생략 (빠름)
```

노트 구성: 가격/기술 → 실적 → 컨센서스·리비전 → 밸류에이션·동종업체 → **이익의 질(SEC XBRL: 계약자산·매출채권·OCF·재고·매입채무)** →
수급(FINRA 공매도, 13F, Form 4) → 옵션(IV, 스트래들 내재 변동폭, OI 벽) → **움직임 분해(섹터 β vs 고유 잔차)** → SEC 공시 핵심 문장 자동 추출 → 뉴스.

## be_monitor.py — $BE Insight Monitor

흔한 가격 알림이 아니라, 집중 포지션에서 **"오늘 무엇이 평소와 다른가"** 를 골라 보내는 봇입니다.

| 모드 | 시점 | 내용 |
|---|---|---|
| `close` | 매 거래일 마감 후 (KST 07:30) | 움직임 분해(섹터 vs BE 고유), IV vs 실현변동성, 전환사채 전환가·강제상환 트리거 거리, 최대 고객 프록시(ORCL), 새 공시/Form 4/Form 144, 뉴스, 일정 |
| `filings` | 매시 (장중~장후) | 새 SEC 공시가 있을 때만 전송. 424B/S-3(희석), 8-K 항목별 분류, Form 144(매도 예정 통지), 13D/G |
| `weekly` | 월요일 KST 08:30 | 레짐(주간 상대성과·잔차 누적·IV), 추정치 7일/30일 드리프트, 투자의견 변화, 공매도 추이, 내부자 순매매, 다음 10-Q 체크포인트 |
| `quarterly` | 새 10-Q/10-K 감지 시 자동 | SEC XBRL로 계약자산·매출채권·OCF·재고·매입채무를 다시 뽑아 전 분기 및 `be_config.json` 기준점과 비교 → 이익의 질 판정 |

### 봇이 보는 "흔하지 않은" 신호
- **움직임 분해**: BE 수익률을 AI 전력 바스켓(VRT·GEV·CEG·VST·OKLO·ETN)+QQQ에 회귀 → 오늘 변동 중 섹터로 설명되는 부분과 BE 고유 잔차를 분리. 잔차 z-score ≥ 1.5면 "BE 고유 원인을 찾아볼 것" 알림. 20일 누적 잔차로 시장이 BE 자체를 재평가 중인지 판단.
- **이익의 질**: 미청구 매출(계약자산)·매출채권 회수 일수·OCF/순이익·매입채무 급증 — 매출 성장이 현금으로 바뀌는지.
- **희석 선행 신호**: Form 144(Form 4보다 먼저), 424B/S-3, 8-K Item 3.02, 전환사채 전환가($194.97)·130% 트리거($253.46) 상회 일수.
- **고객 프록시**: Oracle(ORCL) 동반 약세 = 수요 문제가 아닌 고객 자금조달·신용 우려일 가능성 플래그.
- **옵션**: 30일 IV vs 20일 실현변동성(헤지 비용이 싼지), 실적 만기 스트래들로 본 이벤트 내재 변동폭, OI 집중 행사가(벽).

### GitHub Actions 셋업
1. Secrets: `SLACK_WEBHOOK_URL`(필수), `ANTHROPIC_API_KEY`(선택, weekly/quarterly Claude 메모), `POSITION_SHARES`/`POSITION_COST`(선택, $ 영향 표시)
2. `.github/workflows/be_monitor.yml`이 스케줄 실행. 상태는 `be_state.json`에 커밋됨.
3. 수동 실행: Actions → "BE Insight Monitor" → Run workflow → mode 선택
4. 다른 종목에 쓰려면 `be_config.json`을 복사해 `MONITOR_CONFIG=xyz_config.json`으로 지정

예상 Actions 사용량: filings 11회/일(새 공시 없으면 ~1분) + close 1회(~3분) + weekly ≈ **월 300분 내외**.

> 모든 출력은 데이터 해석이며 투자 조언이 아닙니다.
