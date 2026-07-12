# Copilot Studio Agents - Conversation Chat History using Azure Databricks

## Overview
For many Copilot Studio makers and administrators, pulling conversation transcript data from Dataverse and navigating Application Insights and Azure Log Analytics can be challenging. 

To address this, I created this repository, which allows you to easily view your Copilot Studio Agent conversation history in a Power BI report with a Databricks SQL Backend

Below are sample screenshots of the report.

![Conversation Summary](img/conversationsummary.png)
![Conversation Detail](img/conversationdetail.png)

This repository is the next iteration of the [How to efficiently ingest Dataverse Common Data Model (CDM) tables with Databricks](https://community.databricks.com/t5/technical-blog/how-to-efficiently-ingest-dataverse-common-data-model-cdm-tables/ba-p/66671) article.  For this, we are using Unity Catalog and have the Dataverse Synapse Link write to an ADLS Gen 2 container that is recognized as an external location by Unity Catalog. 

## Assets Contained in this repo

### Databricks Notebooks
This repository contains the following notebooks on how to transform the CSVs created by the Dataverse Synapse Link through the medallion architecture so it can be consumed by the Poer BI Report.
#### Bronze to Silver  
- [CDM to Unity Catalog Auto Loader Ingestion](/src/Autoloader/CDM%20to%20Unity%20Catalog%20Auto%20Loader%20Ingestion.ipynb): This notebook incrementally ingests CDM (Common Data Model) entities exported from Dataverse into Unity Catalog Delta tables using Auto Loader, reading from a UC Volume-backed External Location and applying schemas derived from model.json.
#### Silver to Gold 
- [Conversations - Initial Ingest](src/Autoloader/Conversations%20-%20Initial%20Ingestion.ipynb): This notebook transforms raw Copilot conversation transcripts from the Silver layer (silver.dataverse.conversationtranscript) by parsing nested JSON, exploding individual message parts, joining with user identity data, and writing the enriched, flattened result to a Gold layer Delta table created (gold.copilot.conversations).


## Genie Spaces
You can create a Databricks Genie space to chat with your gold data.  Check out [Set up and manage an AI/BI Genie space](https://learn.microsoft.com/en-us/azure/databricks/genie/set-up) for more information on how to setup an Azure Databricks Genie Space.



## To get started, please perform the following:
1. Set up the Dataverse Synapse Link to an Azure Data Lake for the following tables: **ConversationTranscript** (conversationtranscript) and **User** (systemuser). When finished, you should have a new container in the Azure Data Lake that has a name beginning with "dataverse". For more information, see [Create an Azure Synapse Link for Dataverse with Azure Data Lake](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-data-lake). <BR>
NOTE: Do not setup Incremental Ingestion when creating your Synapse Link.  <BR> 
When finished, the root folder of your new container should look like the screenshot below. <BR>
![ADLS Container](img/ADLS_Container.png)

2. Within Unity Catalog, create an external location that points to the newly created container of the Azure Data Lake from Step 1.  For more information, see [Connect to an Azure Data Lake Storage Gen2 (ADLS Gen2) external location](https://docs.azure.cn/en-us/databricks/connect/unity-catalog/cloud-storage/external-locations-adls#external-location)<BR>
When finished, your external location should look like the screenshot below. <BR>
![External Location](img/external_location.png)

3. Using the Databricks Git Integration with Git Folders  


## Under Construction ##






[CDM Incremental Ingestion](https://github.com/sergioschena/cdm-to-delta)
