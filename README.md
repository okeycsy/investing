# 📊 $HOOD Advanced Monitor v3.0

GitHub Actions 기반 **Robinhood Markets ($HOOD)** 종합 모니터링 봇.
Slack으로 주가, 기술적 지표, 옵션, 공매도, 내부자 거래, 기관 포지션, DCA 시그널을 알려줍니다.

## 🏗️ 아키텍처

```
GitHub Actions (무료 2000분/월)
  ├─ Normal 모드   (장중 매시간)     → 주가 + RSI/MACD + 뉴스 + 내부자
  ├─ Close 모드    (장 마감 후)      → + 옵션 PCR + 공매도 + DCA 시그널
  ├─ 13F 모드      (주 1회 토요일)   → 기관 포지션 변동
  └─ Weekly 모드   (월요일 08:00 KST) → 주간 종합 브리핑 + DCA 스코어
```

## ⏰ 스케줄 (Actions 시간 계산)

| 모드 | 빈도 | 예상 시간/회 | 월간 합계 |
|------|------|-------------|----------|
| Normal | 7회/일 × 22일 | ~1분 | ~154분 |
| Close | 1회/일 × 22일 | ~2분 | ~44분 |
| 13F | 1회/주 × 4주 | ~2분 | ~8분 |
| Weekly | 1회/주 × 4주 | ~2분 | ~8분 |
| **합계** | | | **~214분** |

CGV 봇과 합산해도 2000분 한도의 절반 이하입니다.

## 🚀 셋업

### 1. GitHub Secrets 설정

| Secret | 필수 | 설명 |
|--------|------|------|
| `SLACK_WEBHOOK_URL` | ✅ | Slack Incoming Webhook URL |
| `ANTHROPIC_API_KEY` | ❌ | Claude API 키 (Phase 3 AI 분석용, 없으면 규칙 기반 fallback) |

### 2. 레포에 파일 배치

```
your-repo/
├── .github/workflows/hood_monitor.yml
├── hood_monitor.py
├── requirements.txt
├── state.json
├── weekly_state.json
└── README.md
```

### 3. Actions 활성화

레포 Settings → Actions → General → "Allow all actions" 선택

### 4. 수동 테스트

Actions 탭 → "HOOD Monitor" → "Run workflow" → 모드 선택 → 실행

## 📊 기능 상세

### Phase 1 — 기본 모니터링
- **주가 추적**: Yahoo Finance에서 실시간 가격, 변동률, 거래량
- **RSI/MACD**: 과매도(RSI≤30) 진입 시 DCA 타이밍 알림, MACD 크로스 감지
- **옵션 PCR**: Put/Call Ratio로 시장 심리 판단 (>1.2 = 공포 헤징)
- **공매도 잔고**: FINRA RegSHO 일일 데이터, 숏스퀴즈 가능성 모니터링
- **내부자 거래 강화**: SEC Form 4 XML 직접 파싱, 매수/매도 금액까지 추출
- **뉴스**: Yahoo Finance RSS 최신 헤드라인

### Phase 2 — 기관 추적
- **13F 포지션**: SEC EDGAR에서 분기별 기관 포지션 변동 감지

### Phase 3 — DCA 시그널
- **스코어 (0~100)**: 모든 시그널을 종합한 추가매수 환경 점수
- **Claude AI 분석** (선택): API 키 있으면 AI가 종합 판단, 없으면 규칙 기반
- **주간 브리핑에 통합**: 매주 월요일 아침 한 장으로 정리

## 🎯 DCA 시그널 스코어 기준

| 점수 | 의미 | 권장 |
|------|------|------|
| 80~100 | 강력 매수 시그널 | DCA 추가매수 적극 고려 |
| 60~80 | 매수 우호적 | DCA 추가매수 고려 |
| 40~60 | 중립 | 정기 DCA 유지 |
| 20~40 | 부정적 | 관망 |
| 0~20 | 매수 자제 | DCA 일시 중단 고려 |

### 반영 요인 및 가중치
- RSI 과매도/과매수: ±20
- MACD 크로스: ±10
- 옵션 PCR (역발상): ±10
- 공매도 비율: ±8
- 내부자 매매: ±10
- 가격 급변동: ±5

## ⚠️ 주의사항

- 이 봇은 투자 조언이 아닙니다. 참고 자료로만 활용하세요.
- SEC EDGAR는 rate limit이 있으므로 요청 간 0.2초 딜레이를 유지합니다.
- Yahoo Finance 비공식 API는 변경될 수 있습니다.
- Anthropic API 사용 시 소량의 비용이 발생할 수 있습니다 (월 ~$0.5 이하).

## 🔧 수동 실행

```bash
# 로컬 테스트
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
export ANTHROPIC_API_KEY="sk-ant-..."  # 선택

python hood_monitor.py normal
python hood_monitor.py close
python hood_monitor.py 13f
python hood_monitor.py weekly
```
