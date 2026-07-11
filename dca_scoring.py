from __future__ import annotations

import logging
from typing import Optional

from monitor_models import DCALayerScore, DCAScoreItem, DCATechnicalScore
from technical_indicators import (
    calc_atr as _calc_atr,
    calc_cmf as _calc_cmf,
    calc_daily_hvn as _calc_daily_hvn,
    calc_ema_series as _calc_ema_series,
    calc_mfi as _calc_mfi,
    calc_obv as _calc_obv,
    calc_stochastic as _calc_stochastic,
    calculate_macd,
    calculate_rsi,
    detect_rsi_bullish_divergence as _detect_rsi_bullish_divergence,
)

log = logging.getLogger(__name__)


def calculate_dca_technical_score(
    ohlcv: dict,
    weekly_ohlcv: dict,
    sm=None,      # SafetyMargin (Optional)
) -> Optional[DCATechnicalScore]:
    """
    5-Layer 100점 DCA 기술지표 점수.
    각 항목마다 한글 현황 설명 포함.
    """
    if not ohlcv or len(ohlcv.get("closes", [])) < 30:
        log.warning("DCA technical score: OHLCV 데이터 부족")
        return None

    closes  = ohlcv["closes"]
    highs   = ohlcv["highs"]
    lows    = ohlcv["lows"]
    volumes = ohlcv["volumes"]
    layers  = []

    # ════════════════════════
    # A. Volume / Flow (35pts)
    # ════════════════════════
    items_a: list = []

    # A1. OBV Divergence (10pts)
    obv = _calc_obv(closes, volumes)
    if len(obv) >= 6:
        p_chg = closes[-1] - closes[-6]
        o_chg = obv[-1] - obv[-6]
        if p_chg < 0 and o_chg > 0:
            a1, desc = 10, "가격 5일 하락 중 OBV 상승 → 스마트머니 매집 감지 🟢"
        elif p_chg < 0 and o_chg < 0:
            a1, desc = 0,  "가격·OBV 동반 하락 → 매도 압력 지속 🔴"
        elif p_chg > 0 and o_chg > 0:
            a1, desc = 5,  "가격·OBV 동반 상승 → 정상 추세, 눌림 아님 ⚪"
        else:
            a1, desc = 2,  "가격 상승 중 OBV 약화 → 상승 추세 신뢰도 낮음 🟡"
    else:
        a1, desc = 0, "데이터 부족"
    items_a.append(DCAScoreItem("OBV Divergence", a1, 10, desc))

    # A2. MFI(14) (10pts)
    mfi = _calc_mfi(highs, lows, closes, volumes)
    if mfi is not None:
        if mfi < 20:
            a2, desc = 10, f"MFI {mfi:.1f} — 극과매도, 스마트머니 유입 임박 🟢"
        elif mfi < 30:
            a2, desc = 7,  f"MFI {mfi:.1f} — 과매도 구간, 매수세 증가 가능 🟢"
        elif mfi < 40:
            a2, desc = 4,  f"MFI {mfi:.1f} — 중립 하단, 소폭 매수 우호 🟡"
        elif mfi > 80:
            a2, desc = 0,  f"MFI {mfi:.1f} — 과매수, 차익 실현 압력 🔴"
        else:
            a2, desc = 1,  f"MFI {mfi:.1f} — 중립 구간 ⚪"
    else:
        a2, desc = 0, "MFI 계산 불가"
    items_a.append(DCAScoreItem("MFI(14)", a2, 10, desc))

    # A3. Volume Contraction (8pts)
    if len(volumes) >= 20:
        vol_3d  = sum(v for v in volumes[-3:] if v > 0) / 3
        vol_20d = sum(v for v in volumes[-20:] if v > 0) / 20
        ratio = vol_3d / vol_20d if vol_20d > 0 else 1.0
        pct = ratio * 100
        if ratio < 0.70:
            a3, desc = 8, f"최근 3일 거래량 {pct:.0f}% (20일 평균 대비) — 셀링 약화, 건강한 눌림 🟢"
        elif ratio < 0.85:
            a3, desc = 5, f"최근 3일 거래량 {pct:.0f}% — 소폭 수축, 보통 눌림 🟡"
        elif ratio < 1.00:
            a3, desc = 2, f"최근 3일 거래량 {pct:.0f}% — 평균 수준, 눌림 신호 약함 ⚪"
        else:
            a3, desc = 0, f"최근 3일 거래량 {pct:.0f}% — 거래량 급증, 패닉/돌파 점검 필요 🔴"
    else:
        a3, desc = 0, "데이터 부족"
    items_a.append(DCAScoreItem("Volume Contraction", a3, 8, desc))

    # A4. CMF — Chaikin Money Flow (7pts) [Whale Flow 대체]
    # 1분봉 의존 제거 → 일봉 OHLCV로 기관 매집 강도 정량화
    cmf = _calc_cmf(highs, lows, closes, volumes)
    if cmf is not None:
        if cmf > 0.15:
            a4, desc = 7, f"CMF {cmf:+.3f} — 강한 기관 매집 신호 (스마트머니 유입) 🟢"
        elif cmf > 0.05:
            a4, desc = 5, f"CMF {cmf:+.3f} — 매수 우세, 기관 자금 유입 중 🟢"
        elif cmf > -0.05:
            a4, desc = 2, f"CMF {cmf:+.3f} — 중립, 매수/매도 균형 ⚪"
        elif cmf > -0.15:
            a4, desc = 1, f"CMF {cmf:+.3f} — 매도 우세, 기관 이탈 🟡"
        else:
            a4, desc = 0, f"CMF {cmf:+.3f} — 강한 기관 매도 압력 🔴"
    else:
        a4, desc = 2, "CMF 데이터 부족 ⚪"
    items_a.append(DCAScoreItem("CMF(21) 기관매집", a4, 7, desc))

    layers.append(DCALayerScore(
        "Volume / Flow", "A",
        sum(i.score for i in items_a), 35, items_a))

    # ═══════════════════
    # B. Trend (25pts)
    # ═══════════════════
    items_b: list = []

    # B1. MACD Histogram 바닥 수렴 (10pts)
    if len(closes) >= 35:
        ml, ms, mh = calculate_macd(closes)
        # 최근 3봉 히스토그램 방향 (인덱스 슬라이싱으로 직접 계산)
        hist_prev = [calculate_macd(closes[:-(3 - j)])[2] for j in range(3)]
        converging = all(hist_prev[j] > hist_prev[j - 1] for j in range(1, 3)) and mh < 0
        if converging:
            b1, desc = 10, f"MACD 히스토그램 {mh:.4f} — 음수 구간 3봉 수렴 (바닥 탐색) 🟢"
        elif mh > 0 and all(hist_prev[j] > hist_prev[j - 1] for j in range(1, 3)):
            b1, desc = 7,  f"MACD 히스토그램 {mh:.4f} — 양수 상승 중, 모멘텀 강화 🟡"
        elif mh > 0:
            b1, desc = 5,  f"MACD 히스토그램 {mh:.4f} — 양수 구간 ⚪"
        else:
            b1, desc = 2,  f"MACD 히스토그램 {mh:.4f} — 음수 발산 중, 하락 모멘텀 🔴"
    else:
        b1, desc = 2, "데이터 부족 (35봉 이상 필요)"
    items_b.append(DCAScoreItem("MACD Histogram 수렴", b1, 10, desc))

    # B2. EMA 구조 (10pts)
    e20 = _calc_ema_series(closes, 20)
    e50 = _calc_ema_series(closes, 50)
    e200 = _calc_ema_series(closes, 200)
    cur = closes[-1]
    if e20 and e50 and e200:
        v20, v50, v200 = e20[-1], e50[-1], e200[-1]
        if cur < v20 and v20 > v50 > v200:
            b2, desc = 10, f"가격 < 20EMA < 50EMA < 200EMA 순서 유지 → 장기 추세 건강, 눌림 구간 🟢"
        elif cur < v50 and v50 > v200:
            b2, desc = 7,  f"가격 50EMA 아래, 200EMA 위 → 중기 조정, 장기 추세 양호 🟡"
        elif cur > v20 > v50:
            b2, desc = 5,  f"가격 > 20EMA > 50EMA → 상승 추세, 눌림 아님 ⚪"
        elif v20 < v50:
            b2, desc = 2,  f"20EMA < 50EMA (데드크로스 접근) → 하락 추세 전환 주의 🔴"
        else:
            b2, desc = 4,  "EMA 혼재 — 명확한 추세 없음 ⚪"
    elif e20 and e50:
        v20, v50 = e20[-1], e50[-1]
        b2 = 7 if cur < v20 and v20 > v50 else 3
        desc = f"가격 < 20EMA, 20>50 (200EMA 미계산) 🟡" if b2 == 7 else f"EMA 50일까지 계산 ⚪"
    else:
        b2, desc = 3, "데이터 부족 (최소 50봉 필요)"
    items_b.append(DCAScoreItem("EMA 구조 (20/50/200)", b2, 10, desc))

    # B3. Daily HVN 거리 (5pts) [인트라데이 POC 대체]
    # 60일 일봉 거래량 분포에서 High Volume Node 계산 → 현재가와의 거리
    hvn_pct = _calc_daily_hvn(highs, lows, closes, volumes)
    if hvn_pct is not None:
        if hvn_pct < -3:
            b3, desc = 5, f"Daily HVN 대비 {hvn_pct:+.1f}% — HVN 아래, 역사적 지지 구간 🟢"
        elif hvn_pct < -1:
            b3, desc = 4, f"Daily HVN 대비 {hvn_pct:+.1f}% — HVN 하단 근접, 가치 구간 🟢"
        elif hvn_pct < 1:
            b3, desc = 2, f"Daily HVN 대비 {hvn_pct:+.1f}% — HVN 근처, 중립 ⚪"
        elif hvn_pct < 4:
            b3, desc = 1, f"Daily HVN 대비 {hvn_pct:+.1f}% 위 — HVN이 하단 지지 🟡"
        else:
            b3, desc = 0, f"Daily HVN 대비 {hvn_pct:+.1f}% 위 — 매물대 상단, 고평가 🔴"
    else:
        b3, desc = 2, "Daily HVN 계산 불가 (데이터 부족) ⚪"
    items_b.append(DCAScoreItem("Daily HVN 거리", b3, 5, desc))

    layers.append(DCALayerScore(
        "Trend", "B",
        sum(i.score for i in items_b), 25, items_b))

    # ══════════════════════
    # C. Momentum (20pts)
    # ══════════════════════
    items_c: list = []

    # C1. RSI(14) 일봉 (8pts)
    rsi_val = calculate_rsi(closes)
    if rsi_val <= 25:
        c1, desc = 8, f"RSI {rsi_val:.1f} — 극과매도 (역사적 저점) 🟢"
    elif rsi_val <= 30:
        c1, desc = 7, f"RSI {rsi_val:.1f} — 과매도, 단기 반등 가능성 🟢"
    elif rsi_val <= 40:
        c1, desc = 5, f"RSI {rsi_val:.1f} — 매수 관심 구간 🟡"
    elif rsi_val <= 50:
        c1, desc = 2, f"RSI {rsi_val:.1f} — 중립 하단 ⚪"
    elif rsi_val >= 70:
        c1, desc = 0, f"RSI {rsi_val:.1f} — 과매수, 추격 매수 위험 🔴"
    else:
        c1, desc = 1, f"RSI {rsi_val:.1f} — 중립 ⚪"
    items_c.append(DCAScoreItem("RSI(14) 일봉", c1, 8, desc))

    # C2. Stochastic(14,3,3) (7pts)
    sk, sd = _calc_stochastic(highs, lows, closes)
    if sk is not None and sd is not None:
        in_os = sk < 20 and sd < 20
        crossed = sk > sd
        if in_os and crossed:
            c2, desc = 7, f"Stoch %K={sk:.1f} %D={sd:.1f} — 과매도 골든크로스 🟢"
        elif in_os:
            c2, desc = 4, f"Stoch %K={sk:.1f} %D={sd:.1f} — 과매도, 크로스 대기 🟡"
        elif sk < 50 and crossed:
            c2, desc = 2, f"Stoch %K={sk:.1f} %D={sd:.1f} — 중립대 골든크로스 ⚪"
        elif sk > 80:
            c2, desc = 0, f"Stoch %K={sk:.1f} — 과매수 🔴"
        else:
            c2, desc = 1, f"Stoch %K={sk:.1f} %D={sd:.1f} — 중립 ⚪"
    else:
        c2, desc = 1, "데이터 부족 ⚪"
    items_c.append(DCAScoreItem("Stochastic(14,3,3)", c2, 7, desc))

    # C3. RSI Bullish Divergence (5pts)
    has_div = _detect_rsi_bullish_divergence(closes)
    if has_div:
        c3, desc = 5, "강세 다이버전스 확인 — 가격 저점 갱신, RSI 저점 상승 → 추세 전환 신호 🟢"
    else:
        c3, desc = 0, "강세 다이버전스 미감지 — 현재 없음 ⚪"
    items_c.append(DCAScoreItem("RSI Bullish Divergence", c3, 5, desc))

    layers.append(DCALayerScore(
        "Momentum", "C",
        sum(i.score for i in items_c), 20, items_c))

    # ══════════════════════════════
    # D. Volatility / Entry (12pts)
    # ══════════════════════════════
    items_d: list = []

    # D1. 볼린저 밴드 위치 (7pts) — SafetyMargin 연동
    if sm:
        if sm.bb_signal == "extreme_oversold":
            d1, desc = 7, f"BB 하단 이탈 (하단 대비 {sm.pct_from_lower:+.1f}%) — 극과매도, 평균회귀 압력 🟢"
        elif sm.bb_signal == "oversold":
            d1, desc = 5, f"BB 하단 근접 ({sm.pct_from_lower:+.1f}%) — 과매도 진입 구간 🟡"
        elif sm.bb_signal == "overbought":
            d1, desc = 0, "BB 상단 돌파 — 과열 구간, 매수 자제 🔴"
        else:
            d1, desc = 2, f"BB 중단 구간 ({sm.pct_from_lower:+.1f}%) — 중립 ⚪"
    elif len(closes) >= 20:
        sma20 = sum(closes[-20:]) / 20
        std20 = (sum((c - sma20) ** 2 for c in closes[-20:]) / 20) ** 0.5
        lower = sma20 - 2 * std20
        pfl = (closes[-1] - lower) / lower * 100 if lower > 0 else 0
        if closes[-1] < lower:
            d1, desc = 7, f"BB 하단 이탈 ({pfl:+.1f}%) — 극과매도 🟢"
        elif pfl < 3:
            d1, desc = 5, f"BB 하단 근접 ({pfl:+.1f}%) 🟡"
        else:
            d1, desc = 2, f"BB 중단 ({pfl:+.1f}%) ⚪"
    else:
        d1, desc = 2, "데이터 부족"
    items_d.append(DCAScoreItem("볼린저 밴드 위치", d1, 7, desc))

    # D2. ATR 맥락 (5pts)
    atr = _calc_atr(highs, lows, closes)
    if atr and atr > 0 and len(closes) >= 20:
        recent_high = max(closes[-20:])
        drawdown = abs(closes[-1] - recent_high)
        atr_mult = drawdown / atr
        if atr_mult < 1.5:
            d2, desc = 5, f"20일 고점 대비 낙폭 = ATR {atr_mult:.1f}배 → 건강한 조정 범위 🟢"
        elif atr_mult < 2.5:
            d2, desc = 2, f"20일 고점 대비 낙폭 = ATR {atr_mult:.1f}배 → 확대 조정 🟡"
        else:
            d2, desc = 0, f"20일 고점 대비 낙폭 = ATR {atr_mult:.1f}배 → 과도한 하락, 원인 점검 필요 🔴"
    else:
        d2, desc = 2, "ATR 계산 불가 ⚪"
    items_d.append(DCAScoreItem("ATR 맥락", d2, 5, desc))

    layers.append(DCALayerScore(
        "Volatility / Entry", "D",
        sum(i.score for i in items_d), 12, items_d))

    # ════════════════════════════════════
    # E. Multi-Timeframe Confluence (8pts)
    # ════════════════════════════════════
    items_e: list = []
    w_closes = weekly_ohlcv.get("closes", []) if weekly_ohlcv else []

    # E1. 주봉 RSI (4pts)
    if len(w_closes) >= 14:
        wrsi = calculate_rsi(w_closes)
        if wrsi < 30:
            e1, desc = 4, f"주봉 RSI {wrsi:.1f} — 주봉 과매도, 중기 반등 구간 🟢"
        elif wrsi < 40:
            e1, desc = 3, f"주봉 RSI {wrsi:.1f} — 주봉 과매도 접근 🟡"
        elif wrsi < 50:
            e1, desc = 2, f"주봉 RSI {wrsi:.1f} — 중립 하단 ⚪"
        elif wrsi >= 70:
            e1, desc = 0, f"주봉 RSI {wrsi:.1f} — 주봉 과매수 🔴"
        else:
            e1, desc = 1, f"주봉 RSI {wrsi:.1f} — 중립 ⚪"
    else:
        e1, desc = 1, "주봉 데이터 부족 ⚪"
    items_e.append(DCAScoreItem("주봉 RSI", e1, 4, desc))

    # E2. 주봉 MACD Histogram (4pts)
    if len(w_closes) >= 35:
        wml, wms, wmh = calculate_macd(w_closes)
        if wmh > 0:
            e2, desc = 4, f"주봉 MACD 히스토그램 {wmh:.4f} — 양전환, 주봉 모멘텀 상승 🟢"
        else:
            # 직전봉 히스토그램으로 수렴 여부 확인
            prev_wmh = calculate_macd(w_closes[:-1])[2] if len(w_closes) > 35 else wmh
            if wmh > prev_wmh:
                e2, desc = 3, f"주봉 MACD 히스토그램 {wmh:.4f} — 음수이나 수렴 중 🟡"
            else:
                e2, desc = 0, f"주봉 MACD 히스토그램 {wmh:.4f} — 음수 발산 중 🔴"
    else:
        e2, desc = 1, "주봉 데이터 부족 (35주 이상 필요) ⚪"
    items_e.append(DCAScoreItem("주봉 MACD", e2, 4, desc))

    layers.append(DCALayerScore(
        "Multi-Timeframe", "E",
        sum(i.score for i in items_e), 8, items_e))

    # ═════════════════
    # 총점 + 등급
    # ═════════════════
    total = max(0, min(100, sum(l.pts for l in layers)))
    if total >= 80:
        grade, grade_emoji = "Strong Buy", "🟢🟢"
    elif total >= 60:
        grade, grade_emoji = "Buy", "🟢"
    elif total >= 40:
        grade, grade_emoji = "Neutral", "⚪"
    elif total >= 20:
        grade, grade_emoji = "Caution", "🟡"
    else:
        grade, grade_emoji = "Avoid", "🔴"

    log.info(f"DCA 기술지표 점수: {total}/100 ({grade})")
    return DCATechnicalScore(layers=layers, total=total,
                             grade=grade, grade_emoji=grade_emoji)
