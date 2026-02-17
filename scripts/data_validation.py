"""
Data Validation with Great Expectations
Author: Mohamed SAIFI
Description: Comprehensive data validation for all pipeline layers
"""

import great_expectations as gx
import pandas as pd
import logging
import os
from datetime import datetime


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataValidator:
    """Validate data quality using Great Expectations"""

    def __init__(self, data_context_path: str = "./gx"):
        self.context = gx.get_context()
        self.validation_results = []

    # ============================================================
    # Bronze Layer Expectations
    # ============================================================

    def create_expectations_bronze(self):
        """Create expectations for Bronze layer (raw data)"""

        logger.info("Creating Bronze layer expectation suites")

        # Customers
        customers_suite = self.context.add_or_update_expectation_suite(
            "customers_bronze"
        )

        validator = self.context.sources.pandas_default.read_csv(
            "customers.csv"
        ).get_validator()

        validator.expect_column_values_to_be_unique("customer_id")
        validator.expect_column_values_to_not_be_null("customer_id")
        validator.expect_column_values_to_match_regex(
            "email",
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        )
        validator.expect_column_values_to_not_be_null("name")

        validator.save_expectation_suite(discard_failed_expectations=False)

        # Products
        products_suite = self.context.add_or_update_expectation_suite(
            "products_bronze"
        )

        validator.expect_column_values_to_be_between(
            "price",
            min_value=0,
            max_value=10000,
        )

        validator.expect_column_values_to_be_in_set(
            "category",
            ["Electronics", "Clothing", "Sports", "Home", "Books"],
        )

        validator.save_expectation_suite(discard_failed_expectations=False)

        logger.info("Bronze layer expectation suites created successfully")

    # ============================================================
    # Silver Layer Expectations
    # ============================================================

    def create_expectations_silver(self):
        """Create expectations for Silver layer (cleaned data)"""

        logger.info("Creating Silver layer expectation suites")

        suite = self.context.add_or_update_expectation_suite(
            "sales_enriched_silver"
        )

        critical_columns = [
            "sale_id",
            "customer_id",
            "product_id",
            "total_amount",
        ]

        for col in critical_columns:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
            )

        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="total_amount",
                min_value=0,
            )
        )

        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="quantity",
                min_value=1,
                max_value=100,
            )
        )

        self.context.add_or_update_expectation_suite(
            expectation_suite=suite
        )

        logger.info("Silver layer expectation suites created successfully")

    # ============================================================
    # Gold Layer Expectations
    # ============================================================

    def create_expectations_gold(self):
        """Create expectations for Gold layer (analytics)"""

        logger.info("Creating Gold layer expectation suites")

        suite = self.context.add_or_update_expectation_suite(
            "sales_by_country_gold"
        )

        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="total_sales",
                min_value=0,
            )
        )

        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="number_of_customers",
                min_value=1,
            )
        )

        self.context.add_or_update_expectation_suite(
            expectation_suite=suite
        )

        logger.info("Gold layer expectation suites created successfully")

    # ============================================================
    # Validation Execution
    # ============================================================

    def validate_dataframe(self, df: pd.DataFrame, suite_name: str) -> dict:
        """Validate a dataframe against an expectation suite"""

        logger.info(f"Running validation for suite: {suite_name}")

        batch = self.context.sources.pandas_default.read_dataframe(df)
        results = batch.validate(
            self.context.get_expectation_suite(suite_name)
        )

        self.validation_results.append(
            {
                "suite": suite_name,
                "timestamp": datetime.now().isoformat(),
                "success": results.success,
                "statistics": results.statistics,
            }
        )

        if results.success:
            logger.info(f"Validation successful for suite: {suite_name}")
        else:
            logger.error(f"Validation failed for suite: {suite_name}")

        return results

    # ============================================================
    # Reporting
    # ============================================================

    def generate_validation_report(self) -> str:
        """Generate HTML validation report"""

        os.makedirs("./reports", exist_ok=True)

        report_path = (
            f"./reports/validation_report_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        )

        self.context.build_data_docs()

        logger.info(f"Validation report generated at: {report_path}")

        return report_path

    def get_validation_summary(self) -> dict:
        """Return summary of all validations"""

        summary = {
            "total_validations": len(self.validation_results),
            "successful": sum(
                1 for r in self.validation_results if r["success"]
            ),
            "failed": sum(
                1 for r in self.validation_results if not r["success"]
            ),
            "details": self.validation_results,
        }

        return summary


# ============================================================
# Main Execution
# ============================================================

def main():
    """Run data validation pipeline"""

    logger.info("Starting Data Validation Pipeline")

    validator = DataValidator()

    logger.info("Creating expectation suites")
    validator.create_expectations_bronze()
    validator.create_expectations_silver()
    validator.create_expectations_gold()

    logger.info("Expectation suites created successfully")

    # Example validation
    sample_data = pd.DataFrame(
        {
            "sale_id": [1, 2, 3],
            "customer_id": [101, 102, 103],
            "product_id": [201, 202, 203],
            "quantity": [2, 1, 3],
            "total_amount": [199.99, 49.99, 299.99],
        }
    )

    logger.info("Validating sample dataset")

    results = validator.validate_dataframe(
        sample_data,
        "sales_enriched_silver",
    )

    logger.info(f"Validation success status: {results.success}")

    report_path = validator.generate_validation_report()

    summary = validator.get_validation_summary()

    logger.info("Validation Summary")
    logger.info(f"Total validations executed: {summary['total_validations']}")
    logger.info(f"Successful validations: {summary['successful']}")
    logger.info(f"Failed validations: {summary['failed']}")


if __name__ == "__main__":
    main()
