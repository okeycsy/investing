from __future__ import annotations

from dataclasses import dataclass

from investing_monitor.domain.evidence import (
    EvidenceCandidate,
    EvidenceKind,
    EvidenceProfile,
    extract_filing_items,
)


MATERIAL_8K_ITEMS = {
    "1.01",
    "1.02",
    "2.01",
    "2.02",
    "2.05",
    "2.06",
    "3.01",
    "4.01",
    "4.02",
    "5.02",
}
CONDITIONAL_8K_ITEMS = {"7.01", "8.01"}
ROUTINE_FILING_PATTERNS = (
    "declares quarterly dividend",
    "quarterly cash dividend",
    "results of annual meeting",
    "annual meeting of stockholders",
    "submission of matters to a vote",
)
MATERIAL_EVENT_PATTERNS = (
    "acquisition",
    "acquire",
    "merger",
    "guidance",
    "financial results",
    "earnings",
    "material definitive agreement",
    "credit agreement",
    "debt financing",
    "restructuring",
    "impairment",
    "restatement",
    "non-reliance",
)


@dataclass(frozen=True)
class FilingMateriality:
    material: bool
    reason: str


def assess_filing_materiality(
    candidate: EvidenceCandidate,
    profile: EvidenceProfile,
) -> FilingMateriality | None:
    if candidate.kind is not EvidenceKind.SEC:
        return None
    form = str(candidate.metadata.get("form") or "").upper()
    if form.startswith(("10-Q", "10-K", "20-F")):
        return FilingMateriality(True, "periodic financial report")
    if not form.startswith("8-K"):
        return FilingMateriality(True, "material filing form")

    text = f"{candidate.headline} {candidate.source_text}".casefold()
    if any(pattern in text for pattern in ROUTINE_FILING_PATTERNS):
        return FilingMateriality(False, "routine 8-K event")
    metadata_items = candidate.metadata.get("items") or ()
    if isinstance(metadata_items, str):
        items = set(extract_filing_items(metadata_items))
    else:
        items = {str(item) for item in metadata_items}
    items.update(extract_filing_items(candidate.source_text))
    if items & MATERIAL_8K_ITEMS:
        return FilingMateriality(True, "material 8-K item")
    if items and not (items & CONDITIONAL_8K_ITEMS):
        return FilingMateriality(False, "non-material 8-K item")

    configured_terms = tuple(
        term.casefold()
        for term in (*profile.priority_keywords, *profile.risk_keywords)
        if len(term.strip()) >= 4
    )
    if any(pattern in text for pattern in (*MATERIAL_EVENT_PATTERNS, *configured_terms)):
        return FilingMateriality(True, "material event disclosed under 7.01/8.01")
    if len(candidate.source_text) >= 200:
        return FilingMateriality(False, "8-K contains no configured material event")
    return None
