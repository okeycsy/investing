# Hood Monitor Product Contract

`hood_monitor.py` is a single-stock, long-term investment thesis monitor. VRT is
the reference profile. The product reports material changes and preserves the
user's DCA discipline; it is not a quote screen or an automated trading system.

## Decision Questions

Every visible alert must answer at least one question:

1. What new fact appeared?
2. Why does it matter to the configured investment thesis?
3. Does the existing DCA plan remain intact?
4. What source or next event should be checked?

## Alert Contracts

- `normal`: material news, issuer SEC filings, meaningful insider transactions,
  an abnormal company-specific move, or intraday volume reaching at least 1.5x
  the prior 20-session average. Each volume event is sent at most once per day.
  Price-move alerts begin at 4% and fire again only when the same direction
  enters a new integer percentage band. Reversals never re-arm the same day.
- `close`: one daily decision summary followed by the existing useful detail,
  including the final volume-versus-20-session-average result.
- `weekly`: the week's thesis changes, market direction, relative performance,
  latest volume result, and next checks. It must use a real weekly comparison.
- `13f`: only newly filed positions, with quarter-over-quarter direction when
  the previous position is available.
- `morning`: retained only as a compatibility command; it must not be scheduled.

## Visible Output Rules

- Never show the current stock price or exact daily stock/benchmark return.
- Show only `양전/음전/보합` and benchmark `아웃퍼폼/언더퍼폼/동조`.
- Every displayed news or issuer filing must include a source link.
- Periodic SEC filings must show structured key facts and thesis impact. A raw
  form name or `내용 확인 필요` is not an alert. Related earnings 8-K and 10-Q
  submissions must not be shown as separate events.
- Relative performance must compare both the semiconductor index and an
  equal-weight peer group; unavailable peers are excluded rather than treated as 0%.
- Facts and AI interpretation must be visibly separated.
- AI failure must not consume the news de-duplication key.
- A partial source failure must be disclosed; it must not look like "no change".
- Technical indicators are secondary context. They must not say `Strong Buy`,
  `Buy`, or `Avoid`, and must not prescribe a dollar purchase amount.
  RSI or MACD conditions must never create a standalone intraday alert.
- FINRA daily short volume must not be labeled as short interest.
- Volume alerts must show the current session volume, prior 20-session average,
  ratio, and an explicit `터짐` or `안 터짐` result. The threshold is 1.5x.

## Usability Guardrail

Existing collection depth is preserved unless a source is irrelevant to the
configured profile. Improvements should add a concise decision summary above
useful detail, not replace working detail with a generic status card.
