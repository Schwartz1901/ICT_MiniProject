"""
Medallion Data Pipeline Orchestrator

This script orchestrates the entire data pipeline:
1. MongoDB → Kafka (Producer)
2. Kafka → Snowflake Bronze (Bronze Loader)
3. Bronze → Silver (Silver Transformer)
4. Silver → Gold (Gold Transformer)
5. ML Pipeline (Anomaly Detection)
"""

import argparse
import logging
import sys
from datetime import datetime

from logging_config import setup_logging

setup_logging()
logger = logging.getLogger("Orchestrator")


def run_snowflake_setup():
    """Run Snowflake database/schema/table setup."""
    logger.info("=" * 60)
    logger.info("STEP 0: Snowflake Setup")
    logger.info("=" * 60)

    from snowflake_setup import run_full_setup
    run_full_setup()


def run_cleanup():
    """Clean up all Snowflake databases, tables, and data."""
    logger.info("=" * 60)
    logger.info("CLEANUP: Dropping all databases and tables")
    logger.info("=" * 60)

    from snowflake_client.config import get_connection, close_connection
    from sql import load_query

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Execute cleanup SQL statements one by one
        cleanup_sql = load_query("setup", "cleanup_all")
        statements = [s.strip() for s in cleanup_sql.split(';') if s.strip() and not s.strip().startswith('--')]

        for statement in statements:
            if statement:
                try:
                    logger.info(f"Executing: {statement[:60]}...")
                    cursor.execute(statement)
                except Exception as e:
                    logger.warning(f"Statement failed (may not exist): {e}")

        cursor.close()
        logger.info("Cleanup completed successfully")
    except Exception as e:
        logger.error(f"Cleanup failed: {e}", exc_info=True)
        raise
    finally:
        close_connection()


def run_ingestion():
    """Run MongoDB to Kafka ingestion."""
    logger.info("=" * 60)
    logger.info("STEP 1: MongoDB → Kafka Ingestion")
    logger.info("=" * 60)

    from pipeline_steps import ingestion
    ingestion.run()


def run_bronze(timeout_seconds=60):
    """Run Kafka to Snowflake Bronze loader."""
    logger.info("=" * 60)
    logger.info("STEP 2: Kafka → Snowflake Bronze")
    logger.info("=" * 60)

    from pipeline_steps import bronze
    bronze.run(timeout_seconds=timeout_seconds)


def run_silver():
    """Run Bronze to Silver transformation."""
    logger.info("=" * 60)
    logger.info("STEP 3: Bronze → Silver Transformation")
    logger.info("=" * 60)

    from pipeline_steps import silver
    silver.run()


def run_gold():
    """Run Silver to Gold transformation."""
    logger.info("=" * 60)
    logger.info("STEP 4: Silver → Gold Transformation")
    logger.info("=" * 60)

    from pipeline_steps import gold
    gold.run()


def run_ml():
    """Run ML pipeline."""
    logger.info("=" * 60)
    logger.info("STEP 5: ML Pipeline")
    logger.info("=" * 60)

    from pipeline_steps import ml
    ml.run()


def run_full_pipeline(skip_ingestion=False, bronze_timeout=60):
    """Run the complete Medallion data pipeline."""
    start_time = datetime.now()

    logger.info("#" * 60)
    logger.info("MEDALLION DATA PIPELINE")
    logger.info(f"Started at: {start_time}")
    logger.info("#" * 60)

    try:
        # Step 0: Setup (only if needed)
        run_snowflake_setup()

        # Step 1: Ingestion (MongoDB → Kafka)
        if not skip_ingestion:
            run_ingestion()
        else:
            logger.info("Skipping ingestion step (--skip-ingestion flag)")

        # Step 2: Bronze Loader (Kafka → Snowflake)
        run_bronze(timeout_seconds=bronze_timeout)

        # Step 3: Silver Transformation
        run_silver()

        # Step 4: Gold Transformation
        run_gold()

        # Step 5: ML Pipeline
        run_ml()

        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("#" * 60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Duration: {duration}")
        logger.info("#" * 60)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


def run_transformations_only():
    """Run only the transformation steps (Bronze → Silver → Gold → ML)."""
    start_time = datetime.now()

    logger.info("#" * 60)
    logger.info("TRANSFORMATION PIPELINE (Silver + Gold + ML)")
    logger.info(f"Started at: {start_time}")
    logger.info("#" * 60)

    try:
        run_silver()
        run_gold()
        run_ml()

        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("#" * 60)
        logger.info("TRANSFORMATIONS COMPLETED")
        logger.info(f"Duration: {duration}")
        logger.info("#" * 60)

    except Exception as e:
        logger.error(f"Transformations failed: {e}", exc_info=True)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Medallion Data Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline_orchestrator.py --full                    # Run complete pipeline
  python pipeline_orchestrator.py --full --skip-ingestion   # Skip MongoDB→Kafka
  python pipeline_orchestrator.py --transform               # Run transformations only
  python pipeline_orchestrator.py --setup                   # Setup Snowflake only
  python pipeline_orchestrator.py --cleanup                 # Drop all databases and tables
  python pipeline_orchestrator.py --step ingestion          # Run single step
        """
    )

    parser.add_argument("--full", action="store_true", help="Run complete pipeline")
    parser.add_argument("--transform", action="store_true", help="Run transformations only (Silver→Gold→ML)")
    parser.add_argument("--setup", action="store_true", help="Run Snowflake setup only")
    parser.add_argument("--cleanup", action="store_true", help="Drop all databases, tables, and data (DESTRUCTIVE)")
    parser.add_argument("--skip-ingestion", action="store_true", help="Skip ingestion step")
    parser.add_argument("--bronze-timeout", type=int, default=60, help="Bronze loader timeout in seconds")
    parser.add_argument(
        "--step",
        choices=["ingestion", "bronze", "silver", "gold", "ml"],
        help="Run a single pipeline step"
    )

    args = parser.parse_args()

    if args.cleanup:
        # Confirm before cleanup
        confirm = input("WARNING: This will DELETE ALL databases, tables, and data. Type 'yes' to confirm: ")
        if confirm.lower() == 'yes':
            run_cleanup()
        else:
            logger.info("Cleanup cancelled")
    elif args.setup:
        run_snowflake_setup()
    elif args.full:
        run_full_pipeline(skip_ingestion=args.skip_ingestion, bronze_timeout=args.bronze_timeout)
    elif args.transform:
        run_transformations_only()
    elif args.step:
        if args.step == "ingestion":
            run_ingestion()
        elif args.step == "bronze":
            run_bronze(timeout_seconds=args.bronze_timeout)
        elif args.step == "silver":
            run_silver()
        elif args.step == "gold":
            run_gold()
        elif args.step == "ml":
            run_ml()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
