"""Application configuration."""

import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    # Databricks workspace
    workspace_url: str = os.getenv(
        "DATABRICKS_HOST",
        "https://adb-4116661263058619.19.azuredatabricks.net",
    )
    warehouse_id: str = os.getenv("DATABRICKS_WAREHOUSE_ID", "fe12763ffa92c9b5")

    # Lakebase connection
    lakebase_host: str = os.getenv(
        "LAKEBASE_HOST",
        "ep-square-bird-ea8u2b9t.database.northeurope.azuredatabricks.net",
    )
    lakebase_project: str = os.getenv("LAKEBASE_PROJECT", "data-products-metadata")
    lakebase_database: str = os.getenv("LAKEBASE_DATABASE", "data_products_metadata")
    lakebase_port: int = int(os.getenv("LAKEBASE_PORT", "5432"))

    # Databricks CLI profile (for local dev)
    databricks_profile: str = os.getenv("DATABRICKS_PROFILE", "uc-demo-ws-ne")

    # Tag key used to identify data products
    scan_tag_key: str = os.getenv("SCAN_TAG_KEY", "data_product")


settings = Settings()
