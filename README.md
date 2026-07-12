# Copilot Studio Agents - Conversation Chat History using Azure Databricks

## Overview
For many Copilot Studio makers and administrators, pulling conversation transcript data from Dataverse and navigating Application Insights and Azure Log Analytics can be challenging. 

To address this, I created this repository, which allows you to easily view your Copilot Studio Agent conversation history in a Power BI report with a Databricks SQL Backend

Below are sample screenshots of the report.

![Conversation Summary](img/conversationsummary.png)
![Conversation Detail](img/conversationdetail.png)

This repository is the next iteration of the [How to efficiently ingest Dataverse Common Data Model (CDM) tables with Databricks](https://community.databricks.com/t5/technical-blog/how-to-efficiently-ingest-dataverse-common-data-model-cdm-tables/ba-p/66671) article.  For this, we are using Unity Catalog and have the Dataverse Synapse Link write to an ADLS Gen 2 container that is recognized as an external location by Unity Catalog. 

## To get started, please perform the following:
1. Set up the Dataverse Synapse Link to an Azure Data Lake for the following tables: **ConversationTranscript** (conversationtranscript) and **User** (systemuser). When finished, you should have a new container in the Azure Data Lake that has a name beginning with "dataverse". For more information, see [Create an Azure Synapse Link for Dataverse with Azure Data Lake](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-data-lake). <BR>
NOTE: Do not setup Incremental Ingestion when creating your Synapse Link.  <BR> 
When finished, the root folder of your new container should look like the screenshot below. <BR>
![ADLS Container](img/ADLS_Container.png)

2. Within UNity Catalog, create an external location that points to the newly created container of the Azure Data Lake from Step 1.  For more information, see [Connect to an Azure Data Lake Storage Gen2 (ADLS Gen2) external location](https://docs.azure.cn/en-us/databricks/connect/unity-catalog/cloud-storage/external-locations-adls#external-location)<BR>
When finished, your external location should look like the screenshot below. <BR>
![External Location](img/external_location.png)




## Under Construction ##






[CDM Incremental Ingestion](https://github.com/sergioschena/cdm-to-delta)
