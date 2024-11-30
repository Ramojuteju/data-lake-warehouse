# data-lake-warehouse

1. Resource Group and Storage Account Setup
Created a Resource Group named datalakewarehouse in Azure.
Created a Storage Account named mywarehouse23, enabling hierarchical namespace.
Within the storage account, created 3 containers:
raw: For storing raw, unprocessed data.
curated (or processed): For storing cleaned and processed data.
staging: For storing intermediate data during processing.
2. Azure Data Factory and Databricks Setup
In the datalakewarehouse resource group, created an Azure Data Factory instance named retailfact23.
Provisioned an Azure Databricks workspace named retaildbricks.
Created a Databricks cluster named Ramoju.
In the Databricks workspace, created a folder named retail and added the following notebooks:
retailINC: For data integration tasks.
retailETL: For ETL (Extract, Transform, Load) tasks.
retailMount: For managing mount points and data storage configurations.
3. SQL Database, Key Vault, and GitHub Integration
Created an Azure SQL Database with the following details:
SQL Server: retailserver23
Database: retaildatabase
Created an Azure Key Vault named retailkeyvault to securely store secrets and credentials.
Connected Azure Data Factory to a GitHub repository for version control.
Created the following Linked Services in Azure Data Factory:
HTTP Linked Service: For fetching data from external APIs.
Azure Data Lake Storage Gen2 Linked Service: For storing data in Data Lake.
Azure Key Vault Linked Service: To securely manage secrets (such as database credentials).
SQL Database Linked Service: For connecting to the SQL database.
4. Create Data Factory Pipeline and Linked Services
In Azure Data Factory, created a new pipeline under the Author tab.
Added a Copy Activity in the pipeline under the Move & Transform section.
Source: Configured to fetch data from an external HTTP URL (https://fakestoreapi.com/products).
Sink: Configured to store the data in Azure Data Lake Storage Gen2 as CSV format in the raw container.
5. Set Up Scheduling and Debug the Pipeline
Saved the pipeline, validated it, and added a trigger to schedule it every 1 minute.
Debugged the pipeline to ensure it ran successfully.
Verified the output by checking the raw container in Azure Data Lake Storage for the newly created file.
6. SQL Database to Azure Data Lake Copy Activity
In the Copy Activity, changed the source to the SQL Database.
Configured the connection to Azure Key Vault for securely accessing database credentials (using linked services and secrets).
Selected the appropriate secret from the Key Vault and tested the connection.
Followed the same steps (save, validate, debug) for this copy activity.
After the pipeline ran successfully, verified the data in the raw container.
7. Set Up Databricks Cluster and Secret Scope
In Databricks, created a new cluster and configured it.
In the Databricks settings, created an access token by selecting the developer profile.
Created a Secret Scope in Databricks to store sensitive information such as credentials.
8. Configure Access in Azure Active Directory (AAD)
In Azure Active Directory (Azure AD), generated a new service principal and granted appropriate roles to the Data Factory.
Added the Data Factory name to the Azure AD and created the necessary permissions.
9. Write Code to Process and Move Data
Wrote a Databricks notebook to read data from the raw container, perform necessary cleanup activities, schema changes, and drop any unnecessary columns.
Moved the cleaned data to the curated container in Delta format.
10. Process and Move Data to Staging Container
After writing data to the curated container, wrote a second notebook to read data from the curated container and perform further data transformations.
The processed data was written to the staging container in Delta format, using append mode instead of overwriting.
11. Set Up Azure Synapse Analytics
Created an Azure Synapse Analytics instance named newretailsynapse and connected it to the existing SQL server and database.
Linked Synapse Analytics to GitHub for version control.
In the Synapse workspace, navigated to the Develop section, selected the staging container, and created an external table for the data in the staging container.
Ran the SQL code to check the results of the external table and verified the data.
Repeated the process for a second file in the staging container, creating another external table and verifying the results.
12. GitHub Workflow: Branching and Pull Requests
In the GitHub repository, created a new QA branch from the dev branch.
Submitted a pull request from the dev branch to the QA branch.
Verified that the data was successfully passed to the QA environment after merging the pull request.
