"""
======================================================
E-commerce ETL Pipeline — Standalone Pipeline Runner
======================================================
Runs the full ETL pipeline without Apache Airflow.
Suitable for local development, testing, or cron scheduling.

Usage:
    python scripts/run_pipeline.py --mode full
    python scripts/run_pipeline.py --mode incremental
    python scripts/run_pipeline.py --target bigquery --mode full
======================================================
"""

import os, sys, time, argparse, logging
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# Setup logging to both console and file
log_dir = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file),
    ],
)
logger = logging.getLogger(__name__)


def run_pipeline(target="sqlite", mode="full", generate_data=False):
    """
    Execute the full ETL pipeline: Generate -> Extract -> Transform -> Validate -> Load.

    Args:
        target: 'sqlite' or 'bigquery'
        mode: 'full' or 'incremental'
        generate_data: If True, regenerate sample data before extraction
    """
    pipeline_start = time.time()
    logger.info("=" * 70)
    logger.info("E-COMMERCE ETL PIPELINE — EXECUTION STARTED")
    logger.info(f"  Target: {target}  |  Mode: {mode}  |  Time: {datetime.now()}")
    logger.info("=" * 70)

    try:
        # --- Step 0: Generate Data (optional) ---
        if generate_data:
            logger.info("\n>>> STEP 0: DATA GENERATION")
            step_start = time.time()
            from scripts.generate_data import main as generate_main
            generate_main()
            logger.info(f"    Duration: {time.time() - step_start:.2f}s\n")

        # --- Step 1: Extract ---
        logger.info(">>> STEP 1: EXTRACT")
        step_start = time.time()
        from scripts.extract import extract_all
        raw_data = extract_all()
        logger.info(f"    Duration: {time.time() - step_start:.2f}s\n")

        # --- Step 2: Transform ---
        logger.info(">>> STEP 2: TRANSFORM")
        step_start = time.time()
        from scripts.transform import transform_all
        star_schema = transform_all(raw_data)
        logger.info(f"    Duration: {time.time() - step_start:.2f}s\n")

        # --- Step 3: Validate ---
        logger.info(">>> STEP 3: VALIDATE")
        step_start = time.time()
        from scripts.validate import validate_all
        results = validate_all(star_schema)

        failed = [r for r in results if not r.passed]
        if failed:
            logger.warning(f"    {len(failed)} validation(s) failed — proceeding with caution")
        logger.info(f"    Duration: {time.time() - step_start:.2f}s\n")

        # --- Step 4: Load ---
        logger.info(">>> STEP 4: LOAD")
        step_start = time.time()
        from scripts.load import load_all
        load_all(star_schema, target=target, mode=mode)
        logger.info(f"    Duration: {time.time() - step_start:.2f}s\n")

        # --- Summary ---
        total_time = time.time() - pipeline_start
        logger.info("=" * 70)
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY [OK]  |  Total: {total_time:.2f}s")
        logger.info(f"  Log file: {log_file}")
        logger.info("=" * 70)

    except Exception as e:
        total_time = time.time() - pipeline_start
        logger.error("=" * 70)
        logger.error(f"PIPELINE FAILED [FAIL]  |  After {total_time:.2f}s")
        logger.error(f"  Error: {e}")
        logger.error("=" * 70)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the E-commerce ETL Pipeline")
    parser.add_argument("--target", default="sqlite", choices=["sqlite", "bigquery"],
                        help="Data warehouse target (default: sqlite)")
    parser.add_argument("--mode", default="full", choices=["full", "incremental"],
                        help="Load mode (default: full)")
    parser.add_argument("--generate", action="store_true",
                        help="Regenerate sample data before running")
    args = parser.parse_args()

    run_pipeline(target=args.target, mode=args.mode, generate_data=args.generate)
