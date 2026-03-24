## 📌 Query: Hour Fleet

### 🎯 Purpose

This query analyzes daily operational hours of fleet units by calculating working hours, idle hours, and activity status.

It also derives efficiency metrics through work and idle ratios.

---

### 📁 Data Sources

* **Telemetry Data (Datalake):**

  * Mitsubishi (Fuso)
  * John Deere
  * Liugong
  * Caterpillar

* **Internal Data:**

  * Lark Fleet Master (for enrichment)

---

### ⚙️ Key Logic

#### 1. Latest Equipment Selection

The most recent record for each unit is selected using:

```sql id="hf1"
ROW_NUMBER() OVER (
    PARTITION BY id
    ORDER BY extract_date DESC
)
```

---

#### 2. Daily Work and Idle Hours

For each unit, daily activity is calculated:

* **Work hour** → active operation time
* **Idle hour** → engine running without productive activity

---

#### 3. Active Day Detection

A unit is considered active when:

```sql id="hf2"
(work_hour + idle_hour) > 0
```

---

#### 4. Multi-Source Integration

Data from all manufacturers is standardized and combined:

```sql id="hf3"
UNION ALL
```

---

#### 5. Efficiency Ratio Calculation

Operational efficiency is measured using:

* **Work Ratio (%)**

```sql id="hf4"
work_hour / (work_hour + idle_hour)
```

* **Idle Ratio (%)**

```sql id="hf5"
idle_hour / (work_hour + idle_hour)
```

---

#### 6. Data Enrichment

Fleet data is enriched with:

* Location
* Equipment description
* Classification (class, model)

---

### 🔑 SQL Concepts Used

* Window Functions (`ROW_NUMBER`)
* Aggregation and calculation
* Conditional Logic (`CASE WHEN`)
* `UNION ALL`
* `LEFT JOIN`

---

### 📊 Output Fields

* idNumber
* vehicleName
* vehicleType
* manufacture
* _date
* work_hour
* idle_hour
* is_active_day
* work_ratio
* idle_ratio

---

### 🔍 Key Insights

* Identifies inefficient units with high idle ratios
* Highlights operational performance differences across fleet types
* Enables detailed daily-level monitoring

---

### 🚀 Business Impact

* Supports efficiency improvement initiatives
* Helps reduce idle time and fuel waste
* Improves operational planning and scheduling

---

### 🧠 Notes

This query provides a more granular view of fleet performance compared to utilization analysis, enabling deeper operational insights at the daily level.
