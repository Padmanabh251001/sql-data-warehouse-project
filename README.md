# sql-data-warehouse-project
Building a modern data warehouse with SQL server, including ETL processes, data modeling and analytics

## 📌 Overview
This project demonstrates the design and implementation of a **SQL-based Data Warehouse** using a modern **Medallion Architecture** approach.  
The goal is to ingest raw operational data from multiple source systems, apply structured transformations, and deliver business-ready datasets for analytics and reporting.

The project focuses on data modeling, ETL/ELT concepts, and best practices commonly used in real-world data engineering workflows.

---

## 🗂️ Source Systems
The data used in this project originates from typical enterprise systems:

- **ERP (Enterprise Resource Planning)**
- **CRM (Customer Relationship Management)**

📁 Source data is provided in **CSV format**, simulating real-world data extracts from operational systems.

---

## 🏗️ Data Warehouse Architecture (Medallion Model)

The warehouse is structured using the **Medallion Architecture**, consisting of three layers:

### 🥉 Bronze Layer – Raw Data
- Stores raw data exactly as received from source systems
- No transformations applied
- Acts as a historical and auditable data store

### 🥈 Silver Layer – Cleaned & Standardized Data
- Basic data cleansing and normalization
- Data type corrections
- Removal of duplicates and basic validations
- Prepares data for analytical use

### 🥇 Gold Layer – Business-Level Data
- Business logic and aggregations applied
- Fact and dimension tables
- Data is optimized for reporting, dashboards, and analytics

---

## 🛠️ Technologies Used
- **SQL**
- **CSV files as source data**
- Relational database concepts
- Data modeling (Facts & Dimensions)
- ETL / ELT design principles

---

## 📈 Project Objectives
- Understand end-to-end data warehouse design
- Apply Medallion architecture principles
- Practice SQL transformations at different layers
- Build analytics-ready datasets from raw operational data

---

## 👤 About Me
I am currently working as a **Software Engineer specializing in Revit API development**, where I focus on automation and tool development for AEC workflows.

Over time, my interest has grown in **data engineering, analytics, and large-scale data systems**.  
This project represents my step toward understanding how data is structured, transformed, and delivered at scale in enterprise environments, and reflects my long-term goal of transitioning deeper into **data engineering roles**.

---

## 📄 License
This project is licensed under the **MIT License**.

You are free to use, modify, and distribute this project with proper attribution.

---

## 🚀 Future Enhancements
- Automating data ingestion pipelines
- Adding incremental load logic
- Integrating BI tools for visualization
- Expanding to cloud-based data platforms

---

⭐ If you find this project useful, feel free to star the repository!
