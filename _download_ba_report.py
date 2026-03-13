"""Download the successful MONTHLY Brand Analytics report and inspect format."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))

import asyncio
import json
import structlog, logging

structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG))

REPORT_DOC_ID = "amzn1.spdoc.1.4.eu.9c3087fd-2882-4cc7-923d-0485de49ef5d.T1N0RTJHSWWQH2.5600"

async def main():
    from app.connectors.amazon_sp_api.brand_analytics import BrandAnalyticsClient, parse_search_terms_report

    client = BrandAnalyticsClient(marketplace_id="A1PA6795UKMFR9", sync_profile="diag")

    print("=== Downloading report document ===")
    content = await client.download_report_content(REPORT_DOC_ID)
    print(f"Content length: {len(content)}")
    print(f"Content type: {type(content)}")

    # Save raw for inspection
    with open(r"C:\ACC\_brand_analytics_sample.json", "w", encoding="utf-8") as f:
        f.write(content)
    print("Raw content saved to _brand_analytics_sample.json")

    # Show first 2000 chars
    print(f"\n--- First 2000 chars ---\n{content[:2000]}")

    # Try to parse
    try:
        data = json.loads(content)
        if isinstance(data, list):
            print(f"\n--- Top-level: list with {len(data)} entries ---")
            if data:
                print(f"First entry keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'not dict'}")
                print(f"First entry: {json.dumps(data[0], indent=2, ensure_ascii=False)}")
                if len(data) > 1:
                    print(f"Second entry: {json.dumps(data[1], indent=2, ensure_ascii=False)}")
        elif isinstance(data, dict):
            print(f"\n--- Top-level: dict with keys: {list(data.keys())} ---")
            for k, v in data.items():
                if isinstance(v, list):
                    print(f"  {k}: list with {len(v)} entries")
                    if v:
                        print(f"  First entry: {json.dumps(v[0], indent=2, ensure_ascii=False)}")
                else:
                    print(f"  {k}: {type(v).__name__} = {str(v)[:200]}")
    except Exception as e:
        print(f"JSON parse error: {e}")
        # Maybe it's TSV?
        lines = content.split("\n")
        print(f"\n--- First 5 lines (TSV?) ---")
        for line in lines[:5]:
            print(line[:300])

    # Parse with our parser
    print("\n=== Parsing with parse_search_terms_report ===")
    records = parse_search_terms_report(content, marketplace_id="A1PA6795UKMFR9")
    print(f"Parsed {len(records)} records")
    if records:
        r = records[0]
        print(f"First record: term='{r.search_term}', rank={r.search_frequency_rank}, "
              f"asin={r.asin}, click={r.click_share}, conv={r.conversion_share}, dept={r.department}")
        if len(records) > 5:
            r5 = records[5]
            print(f"Record #5: term='{r5.search_term}', rank={r5.search_frequency_rank}, "
                  f"asin={r5.asin}, click={r5.click_share}, conv={r5.conversion_share}")

if __name__ == "__main__":
    asyncio.run(main())
