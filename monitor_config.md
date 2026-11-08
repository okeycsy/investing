# Monitor Config

종목을 바꾸려면 아래 `ticker` 값을 수정하세요. 예: `ticker: $BE`

ticker: $BE
company_name: Bloom Energy Corp
cik: 0001664703
benchmark: $SOXX
peer_tickers: $FCEL, $GEV, $VRT, $NBIS
app_store_id:
state_dir: .
sec_contact: maybe2213@naver.com

# Investment thesis profile
company_aliases: Bloom Energy
ir_news_url: https://investor.bloomenergy.com/rss/pressrelease.aspx
news_terms: solid oxide fuel cell, fuel cell, onsite power, behind-the-meter power, AI data center power, hydrogen electrolyzer
priority_keywords: orders, backlog, product revenue, gross margin, guidance, data center, capacity expansion, power purchase agreement, free cash flow
risk_keywords: margin pressure, guidance cut, order cancellation, convertible notes, dilution, supply chain, tariff, customer concentration, natural gas price
core_kpis: product revenue, non-GAAP gross margin, backlog, revenue guidance, operating cash flow, manufacturing capacity
profile_context: Solid-oxide fuel cell maker selling onsite, always-on power to AI data centers, utilities and industrials; competes with gas engines and turbines on time-to-power; watch data-center orders and partners (Oracle, AEP, Brookfield, SK ecoplant), backlog, capacity expansion, gross margin, guidance, and financing or dilution.

# Insight signals (close/weekly brief + quarterly earnings-quality checkpoint)
# customer_proxies: 심볼 = 이유; 여러 개는 ';'로 구분
customer_proxies: $ORCL = 최대 고객 Oracle의 AI capex·신용 동향이 수요의 선행 지표; $BN = Brookfield 프로젝트 금융 파트너
# convertible_notes: 라벨 | 전환가 | 상환 트리거 배수 | 최대 전환 주식수(백만); 여러 개는 ';'로 구분
convertible_notes: 0% 전환사채 2030 | 194.97 | 1.30 | 19.55
