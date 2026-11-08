"""
Earnings-quality metrics straight from SEC XBRL company facts.

Why: yfinance statements omit contract assets / customer deposits — exactly the lines that
decide whether reported profit is turning into cash. XBRL has them, quarterly, for every filer.
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

from . import data

# tag candidates (first found wins). flows are de-cumulated to quarterly values.
FLOW_TAGS = {
    "revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"],
    "gross_profit": ["GrossProfit"],
    "op_income": ["OperatingIncomeLoss"],
    "net_income": [
        "NetIncomeLossAvailableToCommonStockholdersBasic",
        "NetIncomeLoss",
        "ProfitLoss",
        "NetIncomeLossAttributableToParentNetOfTaxAndInterest",
    ],
    "ocf": ["NetCashProvidedByUsedInOperatingActivities"],
    "capex": ["PaymentsToAcquirePropertyPlantAndEquipment"],
    "sbc": ["ShareBasedCompensation", "AllocatedShareBasedCompensationExpense"],
}
STOCK_TAGS = {
    "cash": ["CashAndCashEquivalentsAtCarryingValue"],
    "receivables": ["AccountsReceivableNetCurrent", "ContractWithCustomerReceivableAfterAllowanceForCreditLoss"],
    "contract_assets": ["ContractWithCustomerAssetNetCurrent", "ContractWithCustomerAssetNet"],
    "contract_assets_lt": ["ContractWithCustomerAssetNetNoncurrent"],
    "inventory": ["InventoryNet"],
    "payables": ["AccountsPayableCurrent"],
    "deferred_rev": ["ContractWithCustomerLiabilityCurrent", "ContractWithCustomerLiability", "DeferredRevenueCurrent"],
    "customer_deposits": ["CustomerAdvancesAndDeposits", "CustomerDepositsCurrent", "CustomerAdvancesCurrent"],
    "lt_debt": ["LongTermDebtNoncurrent", "LongTermDebt", "ConvertibleNotesPayableNoncurrent"],
    "shares_out": ["CommonStockSharesOutstanding", "EntityCommonStockSharesOutstanding", "WeightedAverageNumberOfDilutedSharesOutstanding"],
    "warranty": ["StandardProductWarrantyAccrual", "ProductWarrantyAccrualClassifiedCurrent"],
}


def _entries(facts: dict, tags: list[str]) -> tuple[str, list[dict]]:
    for t in tags:
        f = facts.get(t)
        if not f:
            continue
        units = f.get("units", {})
        unit = "USD" if "USD" in units else ("shares" if "shares" in units else next(iter(units), None))
        rows = [r for r in units.get(unit, []) if r.get("form") in ("10-Q", "10-K", "10-K/A", "10-Q/A")]
        if rows:
            return t, rows
    return "", []


def _stock_series(rows: list[dict]) -> pd.Series:
    s = {}
    for r in rows:  # later filings restate; keep the latest filed value per period end
        s[r["end"]] = r["val"]
    ser = pd.Series(s)
    ser.index = pd.to_datetime(ser.index)
    return ser.sort_index()


def _quarterly_flow(rows: list[dict]) -> pd.Series:
    """Turn duration facts (3M / YTD / FY mixes) into clean quarterly values."""
    df = pd.DataFrame(rows)
    df["start"] = pd.to_datetime(df["start"])
    df["end"] = pd.to_datetime(df["end"])
    df["days"] = (df["end"] - df["start"]).dt.days
    df = df[(df["days"] > 60) & (df["days"] < 380)]
    df = df.sort_values(["end", "filed"]).drop_duplicates(["start", "end"], keep="last")
    q = {}
    # direct 3-month facts
    for _, r in df[df["days"] <= 100].iterrows():
        q[r["end"]] = r["val"]
    # derive missing quarters from YTD / FY differences (same fiscal-year start)
    for start, g in df.groupby("start"):
        g = g.sort_values("end")
        prev_val, prev_end = None, None
        for _, r in g.iterrows():
            if prev_val is not None and r["end"] not in q and 70 < (r["end"] - prev_end).days < 110:
                q[r["end"]] = r["val"] - prev_val
            prev_val, prev_end = r["val"], r["end"]
    ser = pd.Series(q).sort_index()
    return ser


def quarterly_table(sym: str, quarters: int = 6) -> Optional[pd.DataFrame]:
    """Quarterly table of earnings-quality lines (USD millions)."""
    cik = data.cik(sym)
    if not cik:
        return None
    facts = data.company_facts(cik)
    if not facts:
        return None
    cols = {}
    for name, tags in FLOW_TAGS.items():
        _, rows = _entries(facts, tags)
        if rows:
            cols[name] = _quarterly_flow(rows)
    for name, tags in STOCK_TAGS.items():
        _, rows = _entries(facts, tags)
        if rows:
            cols[name] = _stock_series(rows)
    if not cols:
        return None
    df = pd.DataFrame(cols)
    # keep rows that look like quarter ends with revenue present
    if "revenue" in df.columns:
        df = df[df["revenue"].notna()]
    df = df.tail(quarters)
    money = [c for c in df.columns if c not in ("shares_out",)]
    df[money] = df[money] / 1e6
    if "shares_out" in df.columns:
        df["shares_out"] = df["shares_out"] / 1e6
    return df


def quality_metrics(tbl: pd.DataFrame) -> pd.DataFrame:
    """Derived ratios per quarter."""
    out = pd.DataFrame(index=tbl.index)
    rev = tbl.get("revenue")
    if rev is None:
        return out
    ar = tbl.get("receivables", 0).fillna(0) if "receivables" in tbl else 0
    ca = tbl.get("contract_assets", 0).fillna(0) if "contract_assets" in tbl else 0
    out["unbilled_plus_ar_days"] = (ar + ca) / rev * 91  # days of quarterly revenue tied up
    if "contract_assets" in tbl:
        out["contract_asset_delta_pct_rev"] = tbl["contract_assets"].diff() / rev * 100
    if "ocf" in tbl and "net_income" in tbl:
        out["ocf_to_ni"] = tbl["ocf"] / tbl["net_income"].where(tbl["net_income"] > 0)
    if "ocf" in tbl and "capex" in tbl:
        out["fcf"] = tbl["ocf"] - tbl["capex"].abs()
    if "inventory" in tbl:
        out["inventory_days"] = tbl["inventory"] / (rev - tbl.get("gross_profit", 0)).where(lambda x: x > 0) * 91
    if "payables" in tbl:
        out["payables_delta"] = tbl["payables"].diff()
    if "customer_deposits" in tbl:
        out["customer_deposits"] = tbl["customer_deposits"]
    if "shares_out" in tbl:
        out["shares_yoy_pct"] = tbl["shares_out"].pct_change(4) * 100
    return out
