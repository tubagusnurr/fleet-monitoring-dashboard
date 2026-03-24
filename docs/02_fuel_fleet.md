## 📌 Query: Fuel Fleet

### 🎯 Purpose

This query calculates fuel consumption per fleet unit by analyzing telemetry data and estimating fuel usage over time.

It separates fuel usage into:

* Idle fuel consumption
* Working fuel consumption
* Total fuel consumption

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

#### 1. Data Cleaning & Filtering

Invalid or noisy data is removed:

* Fuel values outside valid range (0–300)
* Invalid sensor readings

---

#### 2. Time Window Aggregation

Fuel data is grouped into 30-minute intervals to standardize measurements:

```sql id="ff1"
date_trunc('hour', timestamp)
+ INTERVAL '30' MINUTE * (...)
```

---

#### 3. Fuel Consumption Calculation (Delta Method)

Fuel usage is calculated using the difference between consecutive readings:

```sql id="ff2"
LAG(fuelLevel) OVER (
    PARTITION BY idNumber
    ORDER BY time
)
```

Fuel is only counted when:

* Fuel level decreases
* Difference is within a reasonable threshold (< 50 liters)

---

#### 4. Distance-Based Logic

Fuel consumption is classified as:

* **Working fuel** → when vehicle is moving (trip_meter > 0)
* **Idle fuel** → when no movement (trip_meter = 0)

---

#### 5. Multi-Source Integration

Fuel data from all manufacturers is combined using:

```sql id="ff3"
UNION ALL
```

---

#### 6. Activity Detection

A unit is considered active if:

* It has working or idle hours > 0

---

### 🔑 SQL Concepts Used

* Window Functions (`LAG`, `ROW_NUMBER`)
* Aggregation (`SUM`)
* Conditional Logic (`CASE WHEN`)
* Time-based transformation
* Data filtering and validation

---

### 📊 Output Fields

* idNumber
* vehicleName
* vehicleType
* manufacture
* _date
* fuel_consumed_idle
* fuel_consumed_working
* fuel_consumed_transport
* fuel_consumed_total
* is_active_day

---

### 🔍 Key Insights

* Identifies fuel inefficiency (high fuel, low activity)
* Detects abnormal fuel usage patterns
* Enables comparison across different fleet types

---

### 🚀 Business Impact

* Supports fuel cost optimization
* Improves operational efficiency
* Helps detect potential fuel misuse or anomalies

---

### 🧠 Notes

Fuel consumption for some manufacturers is derived using indirect calculations (delta method), requiring careful handling of time-series data and anomaly filtering.
