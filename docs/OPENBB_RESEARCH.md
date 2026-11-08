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

## Monitor V2 인사이트 신호 (통합됨)

처음에는 별도 봇(`be_monitor.py`)으로 만들었지만, 저장소에 이미 운영 중인 **Monitor V2**(`src/investing_monitor`)에
신호를 직접 넣는 쪽으로 통합했습니다. 두 개의 봇이 같은 채널에 겹치지 않고, V2의 품질 게이트·outbox·state 브랜치를 그대로 씁니다.

| 위치 | 내용 |
|---|---|
| `domain/insights.py` | 순수 함수: 고유 잔차 분해(기존 민감도 모델 재사용), 옵션 IV·스트래들 내재 변동폭·OI 벽, 전환사채 전환가/트리거, 공매도 잔고, 고객 프록시, SEC XBRL 이익의 질 |
| `adapters/insight_data.py` | Yahoo spark(일봉), yfinance(옵션·공매도·실적일), data.sec.gov companyfacts |
| `application/insights.py` | `InsightService`: close/weekly 라인 생성, `insight_snapshots` 테이블에 전일 상태 저장(IV 급변·공매도 신규 보고·전환가 돌파 감지), 분기 `earnings_quality` 알림 |
| `presentation/insight_messages.py` | 장 마감 브리프 "🔍 오늘 평소와 다른 점", 주간 "🔍 주간 인사이트", "🧪 이익의 질 체크포인트" |

### 실행 시점
- **장 마감 브리프**(`close` task): 거래량 블록 뒤, 레벨 블록 앞에 최대 6줄. 우선순위: 섹터로 설명되지 않는 움직임(z≥1.5) → 20일 누적 잔차 → 고객 프록시 동반 약세 / IV 급변 / 공매도 신규 보고 → IV vs 실현변동성 → 실적 내재 변동폭 → 포지션 환산.
- **주간 리뷰**(`weekly` task): 잔차 누적, IV 추이(주초 대비), 전환사채 거리, 공매도 잔고, 프록시 주간 성과.
- **SEC 폴링**(`sec` task): companyfacts에 새 분기가 잡히면 `earnings_quality` 알림을 분기당 1회 (event_key `{T}:{period_end}:earnings_quality`). 10-Q 공시 알림과 같은 폴링에서 나감.
- 5분 market tick에서는 아무것도 하지 않음 → 추가 요청은 하루 수 건.

### 설정 (`monitor_config.md`)
```
customer_proxies: $ORCL = 최대 고객 Oracle 프록시; $BN = Brookfield 금융 파트너
convertible_notes: 0% 전환사채 2030 | 194.97 | 1.30 | 19.55
```

### 표시 규칙과의 관계
- `presentation/quality.py`의 금지어(RSI/MACD/PCR/FINRA/DCA)를 쓰지 않습니다. 공매도는 "공매도 잔고(거래소 보고)"로, 풋/콜은 "풋/콜 미결제약정"으로 표기합니다.
- 가격·수익률 수치는 V2 현행 코드가 이미 표시하므로 같은 수준으로 보여줍니다. (`HOOD_MONITOR_PRODUCT.md`의 "가격 비노출" 문구는 코드와 이미 어긋나 있어 이 PR에서 건드리지 않았습니다.)
- 실패한 소스는 Actions 결과(`insight_errors`)에만 남고 투자 채널에는 나오지 않습니다.
- `insight_snapshots`는 SQLite(schema v14)에 저장되어 기존 `runtime-state` 브랜치 체크포인트에 포함됩니다. main에 상태 커밋을 만들지 않습니다.

> 모든 출력은 데이터 해석이며 투자 조언이 아닙니다.
