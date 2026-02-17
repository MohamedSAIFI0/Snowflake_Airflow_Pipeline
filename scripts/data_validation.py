import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataValidator:
    """Validate data quality using Great Expectations (GX v1 Fluent API)"""

    def __init__(self):
        self.context = gx.get_context()
        self.validation_results = []

        try:
            self.datasource = self.context.sources.add_pandas("pandas_datasource")
        except Exception:
            self.datasource = self.context.sources.get("pandas_datasource")

    # ============================================================
    # Expectation Suite helpers
    # ============================================================

    def _get_or_create_suite(self, suite_name: str):
        """Return an existing suite or create a new empty one."""
        try:
            return self.context.get_expectation_suite(suite_name)
        except Exception:
            return self.context.add_expectation_suite(suite_name)

    # ============================================================
    # Bronze Layer Expectations
    # ============================================================

    def create_expectations_bronze(self):
        """Create expectations for Bronze layer (raw data)"""
        logger.info("Creating Bronze layer expectation suites")

        # --- Customers ---
        customers_suite = self._get_or_create_suite("customers_bronze")
        customers_suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeUnique(column="customer_id")
        )
        customers_suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
        )
        customers_suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="name")
        )
        customers_suite.add_expectation(
            gx.expectations.ExpectColumnValuesToMatchRegex(
                column="email",
                regex=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            )
        )
        self.context.update_expectation_suite(customers_suite)

        # --- Products ---
        products_suite = self._get_or_create_suite("products_bronze")
        products_suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="price", min_value=0, max_value=10000
            )
        )
        products_suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="category",
                value_set=["Electronics", "Clothing", "Sports", "Home", "Books"],
            )
        )
        self.context.update_expectation_suite(products_suite)

        logger.info("Bronze layer expectation suites created successfully")

    # ============================================================
    # Silver Layer Expectations
    # ============================================================

    def create_expectations_silver(self):
        """Create expectations for Silver layer (cleaned data)"""
        logger.info("Creating Silver layer expectation suites")

        suite = self._get_or_create_suite("sales_enriched_silver")

        for col in ["sale_id", "customer_id", "product_id", "total_amount"]:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
            )

        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="total_amount", min_value=0
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="quantity", min_value=1, max_value=100
            )
        )

        self.context.update_expectation_suite(suite)
        logger.info("Silver layer expectation suites created successfully")

    # ============================================================
    # Gold Layer Expectations
    # ============================================================

    def create_expectations_gold(self):
        """Create expectations for Gold layer (analytics)"""
        logger.info("Creating Gold layer expectation suites")

        suite = self._get_or_create_suite("sales_by_country_gold")

        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="total_sales", min_value=0
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="number_of_customers", min_value=1
            )
        )

        self.context.update_expectation_suite(suite)
        logger.info("Gold layer expectation suites created successfully")

    # ============================================================
    # Validation Execution
    # ============================================================

    def validate_dataframe(self, df: pd.DataFrame, suite_name: str) -> dict:
        """Validate a DataFrame against a named expectation suite."""
        logger.info(f"Running validation for suite: {suite_name}")

        # Add the dataframe as a runtime asset
        asset_name = f"runtime_asset_{suite_name}"
        try:
            data_asset = self.datasource.add_dataframe_asset(name=asset_name)
        except Exception:
            data_asset = self.datasource.get_asset(name=asset_name)

        batch_request = data_asset.build_batch_request(dataframe=df)

        # Build and run a one-off checkpoint
        checkpoint = self.context.add_or_update_checkpoint(
            name=f"checkpoint_{suite_name}",
            validations=[
                {
                    "batch_request": batch_request,
                    "expectation_suite_name": suite_name,
                }
            ],
        )

        checkpoint_result = checkpoint.run()
        success = checkpoint_result.success

        self.validation_results.append(
            {
                "suite": suite_name,
                "timestamp": datetime.now().isoformat(),
                "success": success,
            }
        )

        if success:
            logger.info(f"Validation successful for suite: {suite_name}")
        else:
            logger.error(f"Validation failed for suite: {suite_name}")

        return checkpoint_result

    # ============================================================
    # Reporting
    # ============================================================

    def generate_validation_report(self) -> str:
        """Build GX Data Docs (HTML report)."""
        os.makedirs("./reports", exist_ok=True)
        self.context.build_data_docs()
        report_path = (
            f"./reports/validation_report_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        )
        logger.info(f"Validation report generated: {report_path}")
        return report_path

    def get_validation_summary(self) -> dict:
        """Return summary of all validations run in this session."""
        return {
            "total_validations": len(self.validation_results),
            "successful": sum(1 for r in self.validation_results if r["success"]),
            "failed": sum(1 for r in self.validation_results if not r["success"]),
            "details": self.validation_results,
        }


# ============================================================
# Main Execution
# ============================================================

def main():
    logger.info("Starting Data Validation Pipeline")

    validator = DataValidator()

    logger.info("Creating expectation suites")
    validator.create_expectations_bronze()
    validator.create_expectations_silver()
    validator.create_expectations_gold()

    # Sample dataset for smoke-test
    sample_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3],
            "customer_id": [101, 102, 103],
            "product_id": [201, 202, 203],
            "quantity": [2, 1, 3],
            "total_amount": [199.99, 49.99, 299.99],
        }
    )

    logger.info("Validating sample dataset against sales_enriched_silver suite")
    results = validator.validate_dataframe(sample_data, "sales_enriched_silver")
    logger.info(f"Validation success status: {results.success}")

    validator.generate_validation_report()

    summary = validator.get_validation_summary()
    logger.info(f"Total validations: {summary['total_validations']}")
    logger.info(f"Successful: {summary['successful']}")
    logger.info(f"Failed: {summary['failed']}")


if __name__ == "__main__":
    main()