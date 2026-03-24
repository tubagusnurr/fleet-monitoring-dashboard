## 📌 Query: List Fleet

### 🎯 Purpose

This query builds a unified fleet master dataset by integrating telemetry data from multiple manufacturers with internal fleet records.

It ensures consistency and completeness of fleet information across different data sources.

---

### 📁 Data Sources

* **Telemetry Data (Datalake):**

  * Mitsubishi (Fuso)
  * John Deere
  * Liugong
  * Caterpillar

* **Internal Data:**

  * Lark Fleet Master

---

### ⚙️ Key Logic

#### 1. Latest Data Selection

Each data source contains historical records. The latest record per unit is selected using:

```sql id="lf1"
ROW_NUMBER() OVER (
    PARTITION BY id
    ORDER BY extract_date DESC
)
```

---

#### 2. Multi-Source Integration

Fleet data from different manufacturers is combined into a unified structure:

```sql id="lf2"
UNION ALL
```

---

#### 3. Data Standardization

Key fields are standardized:

* `idNumber`
* `manufacture`
* `vehicleType`

String cleaning is applied using:

* `UPPER()`
* `TRIM()`

---

#### 4. Data Reconciliation

A FULL OUTER JOIN is used to compare:

* Datalake fleet data
* Lark fleet master

```sql id="lf3"
FULL OUTER JOIN
```

---

#### 5. Data Source Classification

Each unit is categorized into:

* **Matched** → exists in both systems
* **Lark Only** → only in internal data
* **Datalake Only** → only in telemetry data

---

### 🔑 SQL Concepts Used

* Window Functions (`ROW_NUMBER`)
* `UNION ALL`
* `FULL OUTER JOIN`
* Data Cleaning (`TRIM`, `UPPER`)
* Conditional Logic (`CASE WHEN`)

---

### 📊 Output Fields

* idNumber
* vehicleName
* vehicleType
* manufacture
* equipment_location
* user
* data_source

---

### 🔍 Key Insights

* Identifies missing fleet records across systems
* Detects inconsistencies in fleet data
* Serves as the foundation for all downstream analysis

---

### 🚀 Business Impact

* Improves data reliability for fleet monitoring
* Enables accurate reporting and dashboarding
* Supports operational decision-making

---

### 🧠 Notes

This query is the core dataset used by other analyses such as fuel consumption, utilization, and operational hours.
