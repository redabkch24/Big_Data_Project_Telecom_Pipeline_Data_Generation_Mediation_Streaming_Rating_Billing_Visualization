# 📞 Telecom CDR Data Processing Pipeline

## 📖 Project Description

This project implements a **Telecom CDR (Call Detail Records)** data processing pipeline that handles both **call** and **SMS** records from generation to visualization.  
It integrates real-time data streaming, mediation, rating, and billing using modern data engineering tools.

---

## 🔄 End-to-End Workflow

### **1. CDR Generation**
- A **CSV file** was generated containing **300 unique phone numbers**.  
- A custom **Python function** was implemented to **generate synthetic CDR records** (calls/SMS) with **randomly injected errors** (e.g., invalid formats, missing fields, etc.).  
- The CDRs are produced and sent to **Kafka topics** using the **Kafka Producer API**.  
  - **Topics:**
    - `raw-cdrs` → stores all incoming raw records  
    - `error-cdrs` → stores invalid/error CDRs  

---

### **2. Mediation (Streaming Layer)**
- Implemented using **Apache Spark Streaming** running on **WSL Ubuntu**.  
- The mediation process consumes messages from the `raw-cdrs` Kafka topic and performs:  
  - ✅ **Validation** of CDR records  
  - ❌ **Error detection and routing** to the `error-cdrs` topic for invalid data  
  - 🧹 **Storage of valid CDRs** into the `clean_cdrs` table in **Microsoft SQL Server (MSSQL)**  

---

### **3. Rating (Batch Processing)**
- The **rating module** runs in **batch mode** using **Apache Spark (Windows environment)**.  
- It reads data from:
  - `clean_cdrs` table  
  - `tariffs` table  
- Based on call duration, type (voice/SMS), and tariff rules, it calculates the **cost per record**.  
- Results are stored in the **`rating_cdrs`** table in **MSSQL**.

---

### **4. Billing (Batch Processing)**
- The **billing module** also runs in **batch mode** on **Spark (Windows)**.  
- It reads from the `rating_cdrs` table and aggregates records by subscriber to calculate:  
  - Total billed amount  
  - Number of calls/SMS  
  - Duration summaries  
- Final results are persisted in the **`billing_cdrs`** table.

---

### **5. Visualization and Analytics**
- **Power BI** is used to visualize the processed data and insights from the billing system.  
- **DAX (Data Analysis Expressions)** is used to create analytical measures such as:  
  - Total Revenue  
  - Average Call Duration  
  - Top Customers by Usage  
  - Call Distribution per Operator  

---

## ⚙️ Technical Stack

| Component | Technology |
|------------|-------------|
| Data Generation | Python (WSL - Ubuntu) |
| Message Broker | Apache Kafka (WSL - Ubuntu) |
| Streaming Processing | Apache Spark Streaming (WSL - Ubuntu) |
| Batch Processing | Apache Spark (Windows) |
| Database | Microsoft SQL Server (Windows) |
| BI & Analytics | Power BI + DAX (Windows) |

---

## 🚀 Key Highlights
- End-to-end **telecom data pipeline** combining **streaming and batch processing**.  
- **Real-time mediation** of CDRs with Kafka and Spark Streaming.  
- **Automated rating and billing** using tariff-based logic.  
- **Interactive dashboards** for business insights with Power BI.  
- Cross-environment integration (**WSL + Windows + MSSQL**).  
