"""
IBKR Shortable Stocks Cross-Reference
--------------------------------------
Parses the saved IBKR HTML, filters to USD only (no *.OLD*),
then cross-references against dtn_equities.csv.

Outputs:
  - ibkr_dtn_exact_matches.csv    : Exact symbol matches (IBKR + DTN overlap)
  - ibkr_dtn_fuzzy_matches.csv    : Similar company names but different symbols (needs review)
"""

import csv
import os
import re
from html.parser import HTMLParser
from difflib import SequenceMatcher

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

HTML_FILE = os.path.join(SCRIPT_DIR, "View Shortable Securities.html")
DTN_FILE = os.path.join(SCRIPT_DIR, "dtn_equities.csv")

OUT_EXACT = os.path.join(SCRIPT_DIR, "ibkr_dtn_exact_matches.csv")
OUT_FUZZY = os.path.join(SCRIPT_DIR, "ibkr_dtn_fuzzy_matches.csv")

FUZZY_THRESHOLD = 0.75  # Minimum name similarity ratio to flag
INCLUDE_PINK = False    # If False, exclude any symbol that has a .PK version (both .PK and base)


class IBKRTableParser(HTMLParser):
    """Parse the IBKR shortable stocks HTML table."""

    def __init__(self):
        super().__init__()
        self.rows = []
        self.in_tbody = False
        self.in_td = False
        self.current_row = []
        self.current_cell = ""

    def handle_starttag(self, tag, attrs):
        if tag == "tbody":
            self.in_tbody = True
        elif tag == "td" and self.in_tbody:
            self.in_td = True
            self.current_cell = ""
        elif tag == "tr" and self.in_tbody:
            self.current_row = []

    def handle_endtag(self, tag):
        if tag == "tbody":
            self.in_tbody = False
        elif tag == "td" and self.in_td:
            self.in_td = False
            self.current_row.append(self.current_cell.strip())
        elif tag == "tr" and self.in_tbody and len(self.current_row) >= 3:
            self.rows.append({
                "symbol": self.current_row[0].strip(),
                "currency": self.current_row[1].strip(),
                "long_name": self.current_row[2].strip(),
            })

    def handle_data(self, data):
        if self.in_td:
            self.current_cell += data


def load_ibkr(filepath):
    """Parse IBKR HTML and return list of dicts."""
    with open(filepath, "r", encoding="utf-8") as f:
        html = f.read()
    parser = IBKRTableParser()
    parser.feed(html)
    print(f"Parsed {len(parser.rows)} total IBKR rows")
    return parser.rows


def filter_ibkr(rows):
    """Keep only USD, exclude *.OLD* symbols."""
    filtered = []
    excluded_currency = 0
    excluded_old = 0
    for row in rows:
        if row["currency"] != "USD":
            excluded_currency += 1
            continue
        if ".OLD" in row["symbol"].upper():
            excluded_old += 1
            continue
        filtered.append(row)
    print(f"Excluded {excluded_currency} non-USD, {excluded_old} .OLD symbols")
    print(f"Remaining IBKR USD symbols: {len(filtered)}")
    return filtered


def load_dtn(filepath):
    """Load DTN equities into lookup dicts."""
    by_symbol = {}  # symbol -> row
    by_name = {}    # normalized name -> list of (symbol, original_name, exchange)

    with open(filepath, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            sym = row[0].strip().strip('"')
            if sym == "SYMBOL":
                continue
            desc = row[1].strip().strip('"') if len(row) > 1 else ""
            exchange = row[3].strip().strip('"') if len(row) > 3 else ""
            listed_market = row[4].strip().strip('"') if len(row) > 4 else ""

            # Skip OTC symbols when INCLUDE_PINK is False
            if not INCLUDE_PINK and listed_market.upper() == "OTC":
                continue

            by_symbol[sym] = {"symbol": sym, "description": desc, "exchange": exchange, "listed_market": listed_market}

            # Name index for fuzzy matching
            norm_name = normalize_name(desc)
            if norm_name and len(norm_name) > 3:
                if norm_name not in by_name:
                    by_name[norm_name] = []
                by_name[norm_name].append({"symbol": sym, "description": desc, "exchange": exchange})

    print(f"Loaded {len(by_symbol)} DTN symbols, {len(by_name)} unique normalized names")
    return by_symbol, by_name


def normalize_name(name):
    """Normalize a company name for comparison."""
    name = name.upper().strip()
    # Remove common suffixes
    for suffix in [" INC", " INC.", " CORP", " CORP.", " CO", " CO.", " LTD",
                   " LTD.", " LLC", " PLC", " SA", " AG", " NV", " SE",
                   " CLASS A", " CLASS B", " CLASS C", " CL A", " CL B", " CL C",
                   " COMMON STOCK", " COMMON", " COM", " ORD", " ORDINARY",
                   " ADR", " ADS", " AMERICAN DEPOSITARY SHARES",
                   " AMERICAN DEPOSITARY SHARE", " -ADR", " - ADR"]:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    # Remove non-alphanumeric
    name = re.sub(r"[^A-Z0-9\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def find_fuzzy_matches(ibkr_unmatched, dtn_by_name, dtn_by_symbol):
    """Find IBKR symbols with similar company names in DTN."""
    fuzzy_matches = []
    dtn_names = list(dtn_by_name.keys())

    for i, ibkr_row in enumerate(ibkr_unmatched):
        ibkr_norm = normalize_name(ibkr_row["long_name"])
        if not ibkr_norm or len(ibkr_norm) < 4:
            continue

        best_ratio = 0
        best_dtn = None

        for dtn_norm in dtn_names:
            ratio = SequenceMatcher(None, ibkr_norm, dtn_norm).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_dtn = dtn_norm

        if best_ratio >= FUZZY_THRESHOLD and best_dtn:
            dtn_entries = dtn_by_name[best_dtn]
            # Pick the one from a major exchange
            dtn_entry = dtn_entries[0]
            for e in dtn_entries:
                if not e["symbol"].endswith(".PK"):
                    dtn_entry = e
                    break

            fuzzy_matches.append({
                "ibkr_symbol": ibkr_row["symbol"],
                "ibkr_long_name": ibkr_row["long_name"],
                "dtn_symbol": dtn_entry["symbol"],
                "dtn_description": dtn_entry["description"],
                "dtn_exchange": dtn_entry["exchange"],
                "similarity": f"{best_ratio:.2f}",
            })

        if (i + 1) % 500 == 0:
            print(f"  Fuzzy matching: {i+1}/{len(ibkr_unmatched)}...")

    return fuzzy_matches


def main():
    # Parse IBKR HTML
    ibkr_all = load_ibkr(HTML_FILE)
    ibkr_usd = filter_ibkr(ibkr_all)

    # Load DTN data
    dtn_by_symbol, dtn_by_name = load_dtn(DTN_FILE)

    # Exact matching
    exact_matches = []
    unmatched = []

    for ibkr_row in ibkr_usd:
        sym = ibkr_row["symbol"].upper()
        if sym in dtn_by_symbol:
            dtn = dtn_by_symbol[sym]
            exact_matches.append({
                "ibkr_symbol": ibkr_row["symbol"],
                "ibkr_long_name": ibkr_row["long_name"],
                "dtn_symbol": dtn["symbol"],
                "dtn_description": dtn["description"],
                "dtn_exchange": dtn["exchange"],
            })
        else:
            unmatched.append(ibkr_row)

    print(f"\nExact symbol matches: {len(exact_matches)}")
    print(f"Unmatched IBKR symbols: {len(unmatched)}")

    # Write exact matches
    with open(OUT_EXACT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["ibkr_symbol", "ibkr_long_name", "dtn_symbol", "dtn_description", "dtn_exchange"],
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()
        writer.writerows(exact_matches)
    print(f"Saved exact matches: {OUT_EXACT}")

    # Fuzzy matching on unmatched
    print(f"\nRunning fuzzy name matching on {len(unmatched)} unmatched symbols...")
    fuzzy = find_fuzzy_matches(unmatched, dtn_by_name, dtn_by_symbol)
    print(f"Fuzzy matches found (>={FUZZY_THRESHOLD:.0%} similarity): {len(fuzzy)}")

    # Write fuzzy matches
    with open(OUT_FUZZY, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["ibkr_symbol", "ibkr_long_name", "dtn_symbol", "dtn_description",
                        "dtn_exchange", "similarity"],
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()
        writer.writerows(sorted(fuzzy, key=lambda x: float(x["similarity"]), reverse=True))
    print(f"Saved fuzzy matches: {OUT_FUZZY}")

    # Summary
    print(f"\n=== Summary ===")
    print(f"IBKR total:           {len(ibkr_all)}")
    print(f"IBKR USD (no .OLD):   {len(ibkr_usd)}")
    print(f"Exact DTN matches:    {len(exact_matches)}")
    print(f"Fuzzy name matches:   {len(fuzzy)}")
    print(f"No match at all:      {len(unmatched) - len(fuzzy)}")


if __name__ == "__main__":
    main()
