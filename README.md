## 🚀 Azure Data Engineering Project - Olist E-commerce Data Pipeline

A cloud-based data engineering project that demonstrates real-world data ingestion, processing, and transformation using Azure services following the **Medallion Architecture (Bronze → Silver → Gold)**.

---

### 📌 Project Architecture

![Architecture Diagram](Architecture%20Diagram.png)

---

### 🧰 Tools & Services Used

- **Azure Data Factory (ADF)** – For orchestrating data ingestion from multiple sources  
- **Azure Data Lake Storage Gen2 (ADLS Gen2)** – For storing raw and processed data  
- **Azure Databricks** – For data transformation using PySpark  
- **MongoDB ([filess.io](https://dash.filess.io/))** – As a NoSQL enrichment source  
- **MySQL ([filess.io](https://dash.filess.io/))** – As a relational source for some datasets  
- **GitHub (via HTTP)** – For hosting static datasets  
- **Azure Synapse Analytics** – For querying transformed data and pushing to Gold layer  
- **Power BI / Tableau / Microsoft Fabric** – For data visualization (optional)

---

### 🧭 Project Flow (Step-by-Step)

1. **🔐 Azure Setup**
   - Created Azure account and resource group: `ecommerce-live`
   - Provisioned necessary services: ADF, ADLS Gen2, Databricks, Synapse

2. **🏗️ Data Lake Setup (Medallion Architecture)**
   - Created ADLS Gen2 Storage: `hsgolistecommstorage`
   - Added container: `olist-data` with directories: `/bronze`, `/silver`, `/gold`

3. **🔄 Data Ingestion using Azure Data Factory**
   - Created ADF pipeline to:
     - Copy data from GitHub (via HTTP) to `/bronze`
     - Ingest data from MySQL using linked service
     - Used JSON config with `csv_relative_url` and `file_name` inside `ForEach` loop
     - Triggered dependent copy activity to move SQL data to `/bronze`

4. **🔗 Configuration for Azure Databricks Access**
   - Registered App in Azure Active Directory  
   - Created Client Secret and assigned **Storage Blob Data Contributor** role  
   - Connected Databricks to ADLS Gen2

5. **🧪 Data Transformation using Azure Databricks**
   - Loaded raw data from ADLS Gen2 `/bronze`
   - Retrieved additional data from MongoDB hosted on [filess.io](https://dash.filess.io/)
   - Merged and cleaned datasets using PySpark
   - Wrote processed data to ADLS Gen2 `/silver` in **Parquet** format

6. **🔍 Data Serving using Azure Synapse**
   - Created Synapse workspace and storage account: `hsg-synapse-storage`
   - Set up Serverless SQL pool and connected to ADLS `/silver`
   - Queried and filtered transformed data (e.g., `order_status = 'delivered'`)
   - Created a view and exported final dataset to `/gold` layer using CETAS  
     👉 [Learn about CETAS in Synapse](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-tables-cetas)

7. **📊 Data Visualization (Optional)**
   - Final datasets from `/gold` are visualization-ready
   - Tools: Power BI / Tableau / Microsoft Fabric

---

### ⚙️ Setup Instructions (Local + Azure)

> **Prerequisites:**  
> - Azure Subscription  
> - Databricks Workspace  
> - GitHub account  
> - MongoDB (hosted or cloud)  
> - MySQL (hosted or cloud)

1. **Clone the Repo**
   ```bash
   git clone https://github.com/yourusername/azure-olist-project.git
   cd azure-olist-project
   ```

2. **Upload Datasets**
   - Push static CSVs to GitHub repo (for HTTP access)
   - Upload one dataset to MySQL DB ([filess.io](https://dash.filess.io/))
   - Push another dataset to MongoDB ([filess.io](https://dash.filess.io/))

3. **Configure Azure Resources**
   - ADF pipelines
   - Linked services (GitHub, MySQL, ADLS Gen2)
   - App Registration & Role Assignment for Databricks

4. **Run Databricks Notebooks**
   - Load data from `/bronze` and MongoDB
   - Clean, transform, join
   - Write output to `/silver` as Parquet

5. **Connect Synapse**
   - Access `/silver` in serverless SQL pool
   - Create views and use CETAS to export to `/gold`  
     (Reference: [CETAS Docs](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-tables-cetas))

6. **Visualize (Optional)**
   - Use Power BI or Tableau to connect to `/gold`

---

### 📂 Folder Structure

```
.
├── notebooks/
│   └── databricks_etl.py
├── adf/
│   └── pipeline.json
├── data/
│   └── sample_input.csv
├── Architecture Diagram.png
├── README.md
└── requirements.txt
```

---

### 📈 Key Concepts

- Medallion Architecture: Bronze (raw) → Silver (cleaned) → Gold (analytics-ready)
- PySpark transformations in Databricks
- CETAS (Create External Table As Select) in Synapse
- JSON-driven dynamic ingestion pipeline in ADF

---

### 🙌 Acknowledgements

- [Azure Docs](https://learn.microsoft.com/en-us/azure/)
- [filess.io](https://dash.filess.io/) for free DB hosting  
- [Kaggle](https://www.kaggle.com/) for datasets  
- Community blogs and tutorials on data lakehouse and medallion design  
