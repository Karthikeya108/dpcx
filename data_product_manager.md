Instructions to Claude Code:

The following instructions are to create a project that will illustrate the implementation of Data Products
using Azure Databricks.

The project will have the following components:

use the databricks SQL warehouse "data-products-poc"

1. A sample Insurance dataset that demonstrated at least three domains and a few sub-domains
    a. The dataset is stored in Azure Databricks as managed delta tables using the Unity Catalog.
    b. The dataset should follow a propoer data modeling approach with proper data types, constraints, and relationships.
    c. The dataset should include at least 20-25 tables and views with approximately 100k records each.
    d. Each table or view should have descriptive name and a description.
    e. Every column in a table or view should have a descriptive name and a description.]
    f. There should be a few tables across domains that have senstive of PII data
    d. The domain is created as a catalogs and the sub-domains are created as schemas within the catalog.
    e. The data products are created as tables or views within the schemas.
    f. The data products can consistute of multiple tables and views.
    g. A data product is identified by a unique "governed tag" of Unity Catalog.
    h. The governed tag should have a key "data_product" and a value that is a unique name for the data product.

2. Create a Lakebase Autoscaling project to store and retrieve metadata about the data products and data contracts
    a. The autoscaling production branch should be 4 to 8 CUs

3. A simple web application:
    a. Create a Databrcks App using the recommendation and resources from https://databricks.github.io/appkit/ and https://github.com/databricks-solutions/apx
    b. Make sure to use https://github.com/shadcn-ui/ui for the UI to make it look sleak
    c. The app should use SQLAlchemy ORM to communicate to Lakebase Autoscaling project.
    c. Use Python FastAPI for backend.
    d. The app should provide backend APIs to read/write data/metadata and perform actions
    d. Home page that provide an overview of the data products such as count, domains, categories, etc.
    e. A page that lists all the data products.
    f. The sournce of truth for all data products is the Lakebase Autoscaling project and the data in Lakebase is populated from Unity Catalog using the provided workspace details
    g. A page that shows the details of a data product.
    h. A page that shows the lineage of a data product.
    i. A paget that lists all the data contracts.
    j. A page that shows the details of a data contract and let's one edit it.
    k. One can create a new data contract from the data product details page.
    l. It should use the ODCS (Open Data Contract Standard) to define the data contracts.
    m. One should be able to upload and download data contracts in the ODCS format.
    n. The application should fetch data products from Azure Databricks using the Databricks REST API / JDBC/ODBC connections.
    o. The application should connect to a Lakebase Autoscaling project to store and retrieve metadata about the data products and data contracts.
    p. The application should have a settings page where one of the configure the following:
        a. Azure Databricks workspace URL
        b. Lakebase Autoscaling API URL
        c. Azure Databricks JDBC/ODBC connection string for the serverless SQL warehouse
        d. A page dedicated to configure the data products scanning that triggers a databricks job to scan the data products using "prefix" or "suffix" of the governed tags in a given metastore.


Use the following resources to accelarate development:
- https://github.com/Karthikeya108/lakebase_demos
- https://github.com/databrickslabs/ontos
- https://github.com/luongnv89/claude-howto/blob/main/clean-code-rules.md