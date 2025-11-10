thonimport argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Dict, Any, Optional

# Make sure local imports work when running as a script
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from crawler.dealroom_crawler import DealroomCrawler  # type: ignore  # noqa: E402
from parser.dealroom_extractor import DealroomExtractor  # type: ignore  # noqa: E402
from parser.utils_json import (  # type: ignore  # noqa: E402
    load_settings,
    read_input_domains,
    write_json_list,
)

def setup_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dealroom Scraper - Fetch and extract company data from Dealroom profiles."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=str(PROJECT_ROOT / "src" / "config" / "settings.json"),
        help="Path to settings.json (default: src/config/settings.json)",
    )
    parser.add_argument(
        "--input",
        type=str,
        help="Path to input domains file (overrides config.paths.input_domains)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Path to output JSON file (overrides config.paths.output_file)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        help="Maximum number of concurrent workers (overrides config.crawler.concurrency)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase log verbosity (can be used multiple times)",
    )
    return parser.parse_args()

def process_single_company(
    identifier: str,
    crawler: DealroomCrawler,
    extractor: DealroomExtractor,
) -> Optional[Dict[str, Any]]:
    logger = logging.getLogger("dealroom-scraper")
    identifier = identifier.strip()
    if not identifier:
        return None

    try:
        html, url = crawler.fetch_company_page(identifier)
        if html is None:
            logger.warning("No HTML fetched for identifier '%s'", identifier)
            return None
        data = extractor.extract_company_data(html, source_url=url)
        logger.info("Extracted data for '%s' (website: %s)", identifier, data.get("website_url"))
        return data
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Failed processing '%s': %s", identifier, exc, exc_info=True)
        return None

def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)
    logger = logging.getLogger("dealroom-scraper")

    settings_path = Path(args.config).expanduser().resolve()
    settings = load_settings(settings_path)

    input_path = (
        Path(args.input).expanduser().resolve()
        if args.input
        else (PROJECT_ROOT / settings["paths"]["input_domains"])
    )
    output_path = (
        Path(args.output).expanduser().resolve()
        if args.output
        else (PROJECT_ROOT / settings["paths"]["output_file"])
    )

    max_workers = (
        args.max_workers
        if args.max_workers and args.max_workers > 0
        else int(settings["crawler"].get("concurrency", 4))
    )

    logger.info("Using config: %s", settings_path)
    logger.info("Reading domains from: %s", input_path)
    logger.info("Writing output to: %s", output_path)
    logger.info("Max workers: %d", max_workers)

    identifiers = read_input_domains(input_path)
    if not identifiers:
        logger.error("No identifiers found in %s", input_path)
        sys.exit(1)

    crawler = DealroomCrawler(settings)
    extractor = DealroomExtractor()

    results: List[Dict[str, Any]] = []

    if max_workers == 1 or len(identifiers) == 1:
        logger.info("Running in sequential mode")
        for identifier in identifiers:
            record = process_single_company(identifier, crawler, extractor)
            if record:
                results.append(record)
    else:
        logger.info("Running with ThreadPoolExecutor (%d workers)", max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id = {
                executor.submit(process_single_company, identifier, crawler, extractor): identifier
                for identifier in identifiers
            }
            for future in as_completed(future_to_id):
                identifier = future_to_id[future]
                try:
                    record = future.result()
                    if record:
                        results.append(record)
                except Exception as exc:  # pylint: disable=broad-except
                    logger.error(
                        "Unhandled error in worker for '%s': %s", identifier, exc, exc_info=True
                    )

    if not results:
        logger.warning("No data extracted for any identifier.")
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        write_json_list(results, output_path)
        logger.info("Wrote %d records to %s", len(results), output_path)

    print(json.dumps({"records": len(results), "output": str(output_path)}, indent=2))

if __name__ == "__main__":
    main()