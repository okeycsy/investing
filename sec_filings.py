from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from urllib.parse import unquote, urljoin, urlsplit

from monitor_models import InsiderTrade

log = logging.getLogger(__name__)


def recent_value(recent: dict, key: str, idx: int) -> str:
    values = recent.get(key, [])
    if idx >= len(values):
        return ""
    return str(values[idx] or "").strip()


def form4_xml_urls_from_submission(accession: str, primary_doc: str, fallback_cik_short: str = "") -> list:
    if not accession or not primary_doc:
        return []

    acc_clean = accession.replace("-", "")
    filing_cik = accession.split("-", 1)[0].lstrip("0") or fallback_cik_short
    primary_name = primary_doc.rsplit("/", 1)[-1]
    if not primary_name.lower().endswith(".xml"):
        return []

    ciks = [filing_cik]
    if fallback_cik_short and fallback_cik_short not in ciks:
        ciks.append(fallback_cik_short)

    return [
        f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{primary_name}"
        for cik in ciks
    ]


def form4_candidates_from_recent(recent: dict, fallback_cik_short: str = "", limit: int = 10) -> list:
    forms = recent.get("form", [])
    candidates = []

    for idx, form in enumerate(forms):
        if str(form).upper() not in {"4", "4/A"}:
            continue

        accession = recent_value(recent, "accessionNumber", idx)
        primary_doc = recent_value(recent, "primaryDocument", idx)
        filing_date = recent_value(recent, "filingDate", idx)
        xml_urls = form4_xml_urls_from_submission(accession, primary_doc, fallback_cik_short)
        if xml_urls:
            candidates.append((filing_date, xml_urls))
        if len(candidates) >= limit:
            break

    return candidates


def find_form4_xml_url(index_html: str, index_url: str) -> str:
    for href in re.findall(r'href=["\']([^"\']+)["\']', index_html):
        parsed = urlsplit(unquote(href))
        path = parsed.path
        name = path.rsplit("/", 1)[-1]
        href_lower = href.lower()
        name_lower = name.lower()

        if not path.lower().endswith(".xml"):
            continue
        if parsed.query or "ixviewer" in href_lower or "/doc/action" in href_lower:
            continue
        if "xsl" in href_lower:
            continue
        if name.startswith("R") or "financial" in name_lower:
            continue

        return urljoin(index_url, href)
    return ""


def parse_form4_xml(xml_text: str, filing_date: str, url: str) -> list:
    trades = []
    try:
        xml_clean = xml_text
        for namespace in [
            'xmlns="http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"',
            'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
        ]:
            xml_clean = xml_clean.replace(namespace, "")
        root = ET.fromstring(xml_clean)

        reporter = root.find(".//reportingOwner/reportingOwnerId")
        if reporter is None:
            reporter = root.find(".//reportingOwnerId")
        filer_name = reporter.findtext("rptOwnerName", "Unknown") if reporter is not None else "Unknown"

        rel = root.find(".//reportingOwner/reportingOwnerRelationship")
        if rel is None:
            rel = root.find(".//reportingOwnerRelationship")

        filer_title = ""
        if rel is not None:
            filer_title = rel.findtext("officerTitle", "").strip()
            if not filer_title:
                is_director = rel.findtext("isDirector", "0").strip()
                is_officer = rel.findtext("isOfficer", "0").strip()
                is_10pct = rel.findtext("isTenPercentOwner", "0").strip()
                if is_director == "1":
                    filer_title = "Director"
                elif is_officer == "1":
                    filer_title = "Officer"
                elif is_10pct == "1":
                    filer_title = "10% Owner"

        for txn in root.findall(".//nonDerivativeTransaction"):
            trade = parse_form4_transaction(txn, filer_name, filer_title, filing_date, url)
            if trade:
                trades.append(trade)
        for txn in root.findall(".//derivativeTransaction"):
            trade = parse_form4_transaction(txn, filer_name, filer_title, filing_date, url)
            if trade:
                trades.append(trade)
    except ET.ParseError:
        log.warning("Form 4 XML parse error")
    except Exception as exc:
        log.warning(f"Form 4 detail error: {exc}")
    return trades


def parse_form4_transaction(txn, filer_name: str, filer_title: str, filing_date: str, url: str):
    try:
        coding = txn.find("transactionCoding")
        txn_code = ""
        if coding is not None:
            txn_code_e = coding.find("transactionCode")
            txn_code = txn_code_e.text.strip() if txn_code_e is not None and txn_code_e.text else ""

        if txn_code in {"C", "J", "G", "W", "Z"}:
            log.debug(f"Form 4 skip (code={txn_code}): {filer_name}")
            return None

        amounts = txn.find("transactionAmounts")
        if amounts is None:
            return None

        shares_e = amounts.find("transactionShares/value")
        price_e = amounts.find("transactionPricePerShare/value")
        code_e = amounts.find("transactionAcquiredDisposedCode/value")

        shares = float(shares_e.text) if shares_e is not None and shares_e.text else 0
        price = float(price_e.text) if price_e is not None and price_e.text else 0
        acq = code_e.text.strip() if code_e is not None and code_e.text else ""

        if shares == 0:
            return None

        if txn_code == "P":
            trade_type = "Purchase"
        elif txn_code == "S":
            trade_type = "Sale"
        elif txn_code == "A":
            trade_type = "Award"
        elif txn_code == "D" or acq == "D":
            trade_type = "Sale"
        else:
            return None

        return InsiderTrade(
            filer=filer_name,
            title=filer_title,
            trade_type=trade_type,
            txn_code=txn_code,
            shares=int(shares),
            price=round(price, 2),
            total_value=round(shares * price, 2),
            date=filing_date,
            url=url,
        )
    except Exception as exc:
        log.debug(f"parse_form4_transaction error: {exc}")
        return None


def extract_ticker_from_infotable(xml_text: str, issuer_keywords: tuple[str, ...] | list[str]) -> tuple:
    try:
        xml_clean = re.sub(r'\s+xmlns[^"]*"[^"]*"', "", xml_text)
        xml_clean = re.sub(r'\s+xmlns[^=]*=\S+', "", xml_clean)
        root = ET.fromstring(xml_clean)

        keyword_set = tuple(str(keyword).upper() for keyword in issuer_keywords)
        for info in root.iter("infoTable"):
            name_elem = info.find("nameOfIssuer")
            if name_elem is None:
                continue
            name_upper = (name_elem.text or "").upper()
            if not any(keyword in name_upper for keyword in keyword_set):
                continue

            shares_elem = info.find("shrsOrPrnAmt/sshPrnamt")
            if shares_elem is None:
                shares_elem = info.find("sshPrnamt")
            value_elem = info.find("value")
            put_call_elem = info.find("putCall")

            if put_call_elem is not None and put_call_elem.text:
                continue

            shares = int(shares_elem.text.replace(",", "")) if shares_elem is not None and shares_elem.text else 0
            value = float(value_elem.text.replace(",", "")) * 1000 if value_elem is not None and value_elem.text else 0.0
            return shares, value, "REPORTED"
    except Exception as exc:
        log.debug(f"infoTable XML parse: {exc}")

    return 0, 0.0, ""
