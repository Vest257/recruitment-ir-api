# main.py — Hays + PageGroup only (official PDFs), simple & reliable.

import io
import re
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import pdfplumber

# -----------------------------
# Config & constants
# -----------------------------
ALLOWED_HOSTS = {
    "www.haysplc.com", "haysplc.com",
    "www.page.com", "page.com",
}

HAYS_RESULTS = "https://www.haysplc.com/investors/results-centre"
PAGE_RESULTS = "https://www.page.com/investors/results-and-presentations"

COMPANY_URLS = {
    "hays": HAYS_RESULTS,
    "pagegroup": PAGE_RESULTS,
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Connection": "keep-alive",
}

# Filter out ESG/policy PDFs by default
NEGATIVE_KEYWORDS = [
    "sustainability", "esg", "human rights", "modern slavery",
    "tax strategy", "gender pay", "gri", "privacy", "policy"
]

app = FastAPI(
    title="Recruitment IR PDF API (Hays & PageGroup)",
    description="Fetch official investor PDFs + extract text/tables/metrics for Hays and PageGroup.",
    version="2.0.0",
)

@app.get("/ok")
def ok():
    return {"ok": True}

# -----------------------------
# Utilities
# -----------------------------
def _get_html(url: str) -> str:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
        r.raise_for_status()
        return r.text
    except requests.HTTPError as e:
        code = getattr(e.response, "status_code", "unknown")
        raise HTTPException(status_code=502, detail=f"Upstream error fetching {url} (status {code})")
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Network error fetching {url}: {e}")

def _same_site_pdf(href: str, base_url: str) -> Optional[str]:
    if not href:
        return None
    full = urljoin(base_url, href)
    if not full.lower().endswith(".pdf"):
        return None
    host = urlparse(full).hostname or ""
    if host not in ALLOWED_HOSTS:
        return None
    return full

def _fetch_pdfs(base_url: str) -> List[Dict[str, str]]:
    html = _get_html(base_url)
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    for a in soup.find_all("a", href=True):
        pdf_url = _same_site_pdf(a.get("href"), base_url)
        if not pdf_url or pdf_url in seen:
            continue
        seen.add(pdf_url)
        title = (a.get_text(strip=True) or pdf_url.split("/")[-1]).strip()
        out.append({"title": title, "pdf_url": pdf_url, "source_page": base_url})
    return out

def _download_pdf(pdf_url: str) -> bytes:
    host = urlparse(pdf_url).hostname or ""
    if host not in ALLOWED_HOSTS:
        raise HTTPException(status_code=400, detail="PDF host not allowed.")
    try:
        r = requests.get(pdf_url, headers=HEADERS, timeout=45)
        r.raise_for_status()
        if "pdf" not in r.headers.get("Content-Type", "").lower() and not pdf_url.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail="URL is not a PDF.")
        return r.content
    except requests.HTTPError as e:
        code = getattr(e.response, "status_code", "unknown")
        raise HTTPException(status_code=502, detail=f"Upstream error fetching PDF (status {code})")
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Network error fetching PDF: {e}")

# -----------------------------
# Models
# -----------------------------
class TextExtractReq(BaseModel):
    pdf_url: str
    pages: Optional[List[int]] = None
    dedupe_whitespace: Optional[bool] = True

class TablesExtractReq(BaseModel):
    pdf_url: str
    pages: Optional[List[int]] = None

class MetricsExtractReq(BaseModel):
    pdf_url: str
    company: str  # "hays" | "pagegroup"
    expected_period_label: Optional[str] = None
    metrics: Optional[List[str]] = None
    countries: Optional[List[str]] = None
    basis_preference_order: Optional[List[str]] = ["Like-for-like","Constant FX","Underlying","Reported"]

# -----------------------------
# Endpoints
# -----------------------------
@app.get("/reports")
def fetch_reports(
    company: str = Query(..., enum=["hays", "pagegroup"]),
    report_type: Optional[str] = None,
    limit: int = Query(5, ge=1, le=20),
    exclude_esg: bool = Query(True, description="Exclude ESG/policy PDFs by default")
):
    base = COMPANY_URLS[company]
    pdfs = _fetch_pdfs(base)

    positives = []
    if report_type:
        positives = [rt.strip().lower() for rt in report_type.split(",") if rt.strip()]

    def keep(p):
        title = p["title"].lower(); url = p["pdf_url"].lower()
        if exclude_esg and any(nk in title or nk in url for nk in NEGATIVE_KEYWORDS): return False
        if not positives: return True
        return any(pk in title or pk in url for pk in positives)

    pdfs = [p for p in pdfs if keep(p)]
    return {"company": company, "results": pdfs[:limit]}

@app.post("/extract/text")
def extract_text(req: TextExtractReq):
    data = _download_pdf(req.pdf_url)
    blocks = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        page_indices = range(len(pdf.pages)) if not req.pages else [p-1 for p in req.pages if 1 <= p <= len(pdf.pages)]
        for idx in page_indices:
            page = pdf.pages[idx]
            txt = page.extract_text() or ""
            if req.dedupe_whitespace:
                txt = re.sub(r"[ \t]+", " ", txt)
                txt = re.sub(r"\n{3,}", "\n\n", txt)
            blocks.append({"page": idx+1, "text": txt})
    return {"pdf_url": req.pdf_url, "blocks": blocks}

@app.post("/extract/tables")
def extract_tables(req: TablesExtractReq):
    data = _download_pdf(req.pdf_url)
    tables_out = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        page_indices = range(len(pdf.pages)) if not req.pages else [p-1 for p in req.pages if 1 <= p <= len(pdf.pages)]
        for idx in page_indices:
            page = pdf.pages[idx]
            words = page.extract_words() or []
            if not words: 
                continue
            rows_map: Dict[int, List[Dict[str, Any]]] = {}
            for w in words:
                key = int(round(w["top"]))
                rows_map.setdefault(key, []).append(w)
            row_keys = sorted(rows_map.keys())
            rows = []
            for k in row_keys:
                row_words = sorted(rows_map[k], key=lambda x: x["x0"])
                row_text = " | ".join(w["text"] for w in row_words)
                if "|" in row_text or re.search(r"\d", row_text):
                    rows.append(row_text)
            if rows:
                cells = []
                for r_i, row in enumerate(rows):
                    cols = [c.strip() for c in row.split("|")]
                    for c_i, c in enumerate(cols):
                        cells.append({"row": r_i, "col": c_i, "text": c})
                tables_out.append({
                    "page": idx+1,
                    "title": f"Detected table-like rows p.{idx+1}",
                    "n_rows": len(rows),
                    "n_cols": max((len(r.split("|")) for r in rows), default=0),
                    "cells": cells
                })
    return {"pdf_url": req.pdf_url, "tables": tables_out}

# simple patterns for a first-pass metric extraction
COUNTRY_PATTERN = r"(Germany|United Kingdom|UK|France|Australia|Netherlands|Belgium|Spain|Portugal|Italy|Japan|China|Hong Kong|Singapore|USA|United States|Canada|Switzerland|Austria|Ireland|Poland|Czech Republic|UAE|United Arab Emirates|New Zealand|India|Brazil|Chile|Mexico)"
VALUE_PATTERN = r"([+\-]?\d+(?:\.\d+)?)\s*%"

@app.post("/extract/metrics")
def extract_metrics(req: MetricsExtractReq):
    data = _download_pdf(req.pdf_url)
    items = []
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            for m in re.finditer(fr"{COUNTRY_PATTERN}.*?(?:net fees|gross profit|fees|consultants|headcount)?.*?{VALUE_PATTERN}", text, flags=re.IGNORECASE):
                country = m.group(1)
                val = float(m.group(len(m.groups())))
                snippet = text[max(0, m.start()-60): m.end()+40].replace("\n", " ")
                basis = None
                if re.search(r"\bLFL\b|like[- ]for[- ]like", snippet, re.I): basis = "Like-for-like"
                elif re.search(r"constant (?:fx|currency)", snippet, re.I):    basis = "Constant FX"
                elif re.search(r"\breported\b", snippet, re.I):                 basis = "Reported"
                metric_name = "Net Fees YoY %" if re.search(r"net fees|fees", snippet, re.I) else "Gross Profit YoY %"
                norm_country = "United Kingdom" if country == "UK" else country
                if req.metrics and metric_name not in req.metrics: continue
                if req.countries and norm_country not in req.countries: continue
                items.append({
                    "company": {"hays":"Hays plc","pagegroup":"PageGroup"}[req.company],
                    "report_title": None, "report_date": None,
                    "country": norm_country, "region": None,
                    "metric": metric_name, "value": val, "unit": "%",
                    "period_label": req.expected_period_label or "", "basis": basis,
                    "source_text": snippet.strip(), "page": i,
                    "table_title": None, "footnote_refs": []
                })

    not_disclosed = []
    if req.countries:
        found = {it["country"] for it in items}
        for c in req.countries:
            if c not in found:
                not_disclosed.append(c)

    recheck_performed = False
    if len(not_disclosed) >= 3:
        recheck_performed = True
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                for c in list(not_disclosed):
                    if re.search(fr"\b{re.escape(c)}\b", text):
                        not_disclosed.remove(c)

    return {
        "pdf_url": req.pdf_url,
        "report_title": None,
        "report_date": None,
        "company": req.company,
        "period_label": req.expected_period_label or "",
        "items": items,
        "not_disclosed": not_disclosed,
        "recheck_performed": recheck_performed
    }
