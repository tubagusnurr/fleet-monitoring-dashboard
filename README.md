# 🚜 Fleet Monitoring Dashboard

## 📌 Overview

This project presents an end-to-end fleet monitoring system built using SQL to analyze operational performance across multiple equipment units.

It integrates telemetry data from various manufacturers with internal fleet records to create a unified, analysis-ready dataset.

---

## 🎯 Objectives

* Monitor fleet availability and distribution
* Analyze fuel consumption efficiency
* Measure equipment utilization
* Track operational working hours

---

## 🧩 Data Components

### 1. Fleet Master (`listFleet`)

* Integrates multi-source fleet data
* Performs data reconciliation between systems
* Identifies missing or unmatched units

### 2. Fuel Analysis (`fuelFleet`)

* Calculates fuel consumption using time-series logic
* Separates idle vs working fuel usage
* Detects inefficiencies and anomalies

### 3. Utilization Analysis (`utilizationFleet`)

* Measures active usage over a 30-day window
* Identifies underutilized equipment
* Supports operational optimization

### 4. Hour Analysis (`hourFleet`)

* Tracks daily working and idle hours
* Calculates efficiency ratios (work vs idle)
* Enables detailed performance monitoring

---

## ⚙️ Data Pipeline

Raw Data (Telemetry & Lark)
→ Data Cleaning & Transformation (SQL)
→ Aggregation & KPI Calculation
→ Dashboard Visualization (Power BI)

---

## 📊 Key Insights

* Identified units with high fuel consumption but low utilization → potential inefficiency
* Detected underutilized fleet across multiple locations
* Highlighted operational imbalance using work vs idle hour ratios
* Found inconsistencies between telemetry data and internal fleet records

---

## 🛠️ Tech Stack

* SQL (Window Functions, Aggregation, Data Transformation)
* Data Warehouse / Datalake
* Power BI (Visualization)

---

## 📁 Project Structure

* `sql/` → Core transformation queries
* `docs/` → Detailed documentation per query
* `assets/` → Dashboard preview (optional)

---

## 🚀 Key Strengths of This Project

* Multi-source data integration
* Time-series analysis (fuel & activity tracking)
* Data cleaning and standardization
* Business-driven KPI generation
* End-to-end analytical pipeline

---

## 👤 Author

**Tubagus Nur Rahmat Putra**
GIS Analyst | Remote Sensing Specialist | Data Enthusiast
