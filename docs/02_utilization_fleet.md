## 📌 Query: Utilization Fleet

### 🎯 Purpose

This query evaluates fleet utilization by measuring how actively each unit is used over a 30-day period.

It helps identify underutilized equipment and supports better operational planning.

---

### 📁 Data Sources

* **Telemetry Data (Datalake):**

  * Mitsubishi (Fuso)
  * John Deere
  * Liugong
  * Caterpillar

* **Internal Data:**

  * Lark Fleet Master (for location and classification)

---

### ⚙️ Key Logic

#### 1. Latest Equipment Selection

For each manufacturer, the most recent unit data is selected using:

```sql id="uf1"
ROW_NUMBER() OVER (
    PARTITION BY serialNumber
    ORDER BY extract_date DESC
)
```

---

#### 2. Work Hour Calculation

Daily activity is calculated by combining:

* Working hours
* Idle hours

---

#### 3. Active Day Definition

A unit is considered active if:

```sql id="uf2"
work_hour > 0
```

---

#### 4. 30-Day Activity Window

Only data from the last 30 days is used to evaluate utilization.

---

#### 5. Utilization Classification

Units are categorized based on activity:

* **Underutilized** → active days < 10
* **Utilized** → active days ≥ 10

---

#### 6. Aggregation by Dimension

Results are grouped by:

* Manufacture
* Vehicle type
* Location
* User / department

---

### 🔑 SQL Concepts Used

* Window Functions (`ROW_NUMBER`)
* Aggregation (`SUM`, `COUNT`)
* Conditional Logic (`CASE WHEN`)
* `UNION ALL`
* `LEFT JOIN`

---

### 📊 Output Metrics

* total_unit
* total_underutilized_unit
* total_utilized_unit

---

### 🔍 Key Insights

* Identifies idle or underutilized fleet units
* Highlights imbalance in fleet distribution across locations
* Supports resource optimization

---

### 🚀 Business Impact

* Reduces operational inefficiencies
* Improves asset allocation
* Supports data-driven fleet management decisions

---

### 🧠 Notes

This query standardizes activity measurements across multiple telemetry systems with different data structures.
