"""
DTN IQ Symbol Scraper
---------------------
Attaches to Edge (running with --remote-debugging-port=9222),
reads HTML Table results page by page, appends unique rows to dtn_equities.csv.

Setup:
  1. Close all Edge windows
  2. Run:  msedge --remote-debugging-port=9222
  3. Navigate to https://ws1.dtn.com/IQ/Search/#
  4. Set filters (Exchange, Equity, no options/spreads, HTML Table), click Search
  5. Run this script:  python dtn_scraper.py

Requires: pip install selenium
"""

import csv
import os
import time
import sys
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
)

OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dtn_equities.csv")
CSV_HEADER = ["SYMBOL", "DESCRIPTION", "SECURITY_TYPE", "EXCHANGE", "LISTED_MARKET"]
PAGE_LOAD_WAIT = 5  # seconds to wait for results to update after clicking Next


def attach_to_edge():
    """Connect to an already-running Edge instance on port 9222 and switch to the DTN tab."""
    opts = Options()
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    driver = webdriver.Edge(options=opts)
    print(f"Attached to Edge — current URL: {driver.current_url}")

    # Switch to the tab that has the DTN site
    for handle in driver.window_handles:
        driver.switch_to.window(handle)
        if "dtn.com" in driver.current_url.lower():
            print(f"Found DTN tab: {driver.current_url}")
            return driver

    # If no DTN tab found, stay on current and warn
    print("WARNING: No tab with dtn.com found. Make sure the DTN page is open.")
    print(f"Available tabs: {[driver.switch_to.window(h) or driver.current_url for h in driver.window_handles]}")
    return driver


def load_existing_symbols(filepath):
    """Load existing CSV symbols into a set for deduplication."""
    seen = set()
    if os.path.exists(filepath):
        with open(filepath, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if row and row[0].strip().strip('"') != "SYMBOL":
                    seen.add(row[0].strip().strip('"'))
        print(f"Loaded {len(seen)} existing unique symbols from {filepath}")
    return seen


def get_table_rows(driver):
    """Extract rows from the HTML table on the results page. Returns list of 5-element lists."""
    rows = []

    # Check inside iframes first (DTN often uses them for results)
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    for iframe in iframes:
        try:
            driver.switch_to.frame(iframe)
            rows = _extract_rows_from_tables(driver)
            driver.switch_to.default_content()
            if rows:
                return rows
        except Exception:
            driver.switch_to.default_content()

    # Try directly on the page
    rows = _extract_rows_from_tables(driver)
    return rows


def _extract_rows_from_tables(driver):
    """Find HTML tables and extract data rows with 5 columns via single JS call."""
    rows = driver.execute_script("""
        var results = [];
        var tables = document.querySelectorAll('table');
        for (var t = 0; t < tables.length; t++) {
            var trs = tables[t].querySelectorAll('tr');
            for (var r = 0; r < trs.length; r++) {
                var tds = trs[r].querySelectorAll('td');
                if (tds.length >= 5) {
                    var first = tds[0].innerText.trim();
                    if (first && first.toUpperCase() !== 'SYMBOL') {
                        results.push([
                            first,
                            tds[1].innerText.trim(),
                            tds[2].innerText.trim(),
                            tds[3].innerText.trim(),
                            tds[4].innerText.trim()
                        ]);
                    }
                }
            }
            if (results.length > 0) return results;
        }
        return results;
    """)
    return rows or []


def click_next(driver):
    """Click the Next button. Returns True if clicked, False if not found (last page)."""
    next_selectors = [
        "//a[contains(text(),'Next')]",
        "//input[@value='Next']",
        "//button[contains(text(),'Next')]",
        "//a[contains(@class,'next')]",
        "//input[contains(@value,'Next')]",
        "//a[contains(text(),'next')]",
        "//span[contains(text(),'Next')]/parent::a",
    ]
    for xpath in next_selectors:
        try:
            btn = driver.find_element(By.XPATH, xpath)
            if btn.is_displayed() and btn.is_enabled():
                btn.click()
                return True
        except (NoSuchElementException, StaleElementReferenceException):
            continue
    return False


def wait_for_page_change(driver, old_first_symbol, timeout=15):
    """Wait until the first symbol on the page changes."""
    start = time.time()
    while time.time() - start < timeout:
        rows = get_table_rows(driver)
        if rows and rows[0][0] != old_first_symbol:
            return True
        time.sleep(0.5)
    return False


def debug_page(driver):
    """Print page structure info to help identify the right selectors."""
    print("\n=== DEBUG: Page Analysis ===")
    print(f"URL: {driver.current_url}")
    print(f"Title: {driver.title}")

    # Check for iframes
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    print(f"Iframes found: {len(iframes)}")
    for i, iframe in enumerate(iframes):
        src = iframe.get_attribute("src") or "(no src)"
        print(f"  iframe[{i}]: src={src}")

    # Dump all element IDs on the page
    ids = driver.execute_script(
        "return Array.from(document.querySelectorAll('[id]')).map(e => e.tagName + '#' + e.id).slice(0, 50)"
    )
    print(f"Elements with IDs: {ids}")

    # Get a snippet of body text
    body = driver.find_element(By.TAG_NAME, "body").text
    preview = body[:500] if body else "(empty)"
    print(f"Body text preview:\n{preview}")

    # Try innerHTML of any large divs
    big_divs = driver.execute_script("""
        return Array.from(document.querySelectorAll('div, td, pre, textarea'))
            .filter(e => e.innerText && e.innerText.length > 100 && e.innerText.includes(','))
            .map(e => ({tag: e.tagName, id: e.id, cls: e.className, len: e.innerText.length, preview: e.innerText.substring(0, 200)}))
            .slice(0, 10)
    """)
    print(f"Large elements with commas: {big_divs}")
    print("=== END DEBUG ===\n")


def main():
    driver = attach_to_edge()

    # Run debug on first page to understand structure
    debug_page(driver)

    seen = load_existing_symbols(OUTPUT_FILE)
    initial_seen_count = len(seen)

    # Write header if file is new/empty
    write_header = not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0

    page = 1
    total_new = 0

    print(f"\nStarting scrape. Output: {OUTPUT_FILE}\n")

    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        if write_header:
            writer.writerow(CSV_HEADER)

        while True:
            # Read current page
            rows = get_table_rows(driver)
            if not rows:
                print(f"Page {page}: No table data found. Check the page manually.")
                input("Press Enter after fixing the page, or Ctrl+C to quit...")
                continue

            new_rows = [r for r in rows if r[0] not in seen]

            for row in new_rows:
                writer.writerow(row)
                seen.add(row[0])

            total_new += len(new_rows)
            print(f"Page {page}: {len(rows)} rows read, {len(new_rows)} new, {len(rows) - len(new_rows)} duplicates skipped | Running total new: {total_new}")
            f.flush()

            old_first_symbol = rows[0][0] if rows else ""

            # Try clicking Next
            if not click_next(driver):
                print(f"\nNo 'Next' button found — end of results after {page} pages.")
                break

            # Wait for page to update
            if not wait_for_page_change(driver, old_first_symbol):
                print(f"Warning: Page content didn't change after clicking Next. Retrying...")
                time.sleep(PAGE_LOAD_WAIT)

            page += 1

    grand_total = len(seen)
    print(f"\nDone! {total_new} new rows added this run.")
    print(f"Total unique symbols in {OUTPUT_FILE}: {grand_total}")
    print("Switch to the next exchange, click Search, and run this script again to append.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nStopped by user. Data saved so far is in dtn_equities.csv.")
        sys.exit(0)
