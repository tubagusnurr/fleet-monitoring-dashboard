-- ============================================
-- Query Name : Hour Fleet
-- File Name  : 04_hour_fleet.sql
-- Author     : Tubagus Nur Rahmat Putra
-- Description:
--   This query calculates daily operating hours,
--   idle hours, and activity status for fleet units
--   across multiple manufacturers.
--
--   It also derives work and idle ratios to measure
--   operational efficiency.
--
-- Key Metrics:
--   - work_hour
--   - idle_hour
--   - is_active_day
--   - work_ratio (%)
--   - idle_ratio (%)
--
-- ============================================

WITH 

-- ============================================
-- MITSUBISHI (FUSO): Latest Devices
-- ============================================
latest_devices AS (
    SELECT 
        imei,
        chassis_number,
        vehicle_name,
        vehicle_type
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY imei
                   ORDER BY extract_date DESC
               ) AS rn
        FROM "prod_datalake"."view_telemetry_ktbfuso_list_devices"
    ) t
    WHERE rn = 1
),

-- ============================================
-- MITSUBISHI: Daily Summary
-- ============================================
daily_summary AS (
    SELECT 
        CAST(SUBSTR(_date, 1, 10) AS DATE) AS date_record,
        ROUND(TRY_CAST(long_drive AS DOUBLE) / 3600, 2) AS long_drive,
        ROUND(TRY_CAST(long_idle AS DOUBLE) / 3600, 2) AS long_idle,
        imei
    FROM "prod_datalake"."view_telemetry_ktbfuso_runner_api_daily_summary"
),

table1 AS (
    SELECT
        d.chassis_number    AS idNumber,
        d.vehicle_name      AS vehicleName,
        d.vehicle_type      AS vehicleType,
        'MITSUBISHI'        AS manufacture,
        ds.date_record      AS _date,
        ds.long_drive       AS work_hour,
        ds.long_idle        AS idle_hour,
        CASE 
            WHEN (ds.long_drive + ds.long_idle) > 0 THEN 1 
            ELSE 0 
        END AS is_active_day
    FROM latest_devices d
    LEFT JOIN daily_summary ds ON d.imei = ds.imei
),

-- ============================================
-- JOHN DEERE
-- ============================================
latest_equipment AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY serialNumber
            ORDER BY extract_date DESC
        ) AS rn
    FROM "prod_datalake"."view_telemetry_john_deere_monthly_equipment"
    WHERE telematicsCapable = 'true'
),

machine_utilization AS (
    SELECT
        serial_number           AS serialNumber,
        DATE(intervalEndDate)   AS _date,
        SUM(idle/3600.0)        AS idle_hour,
        SUM(working/3600.0)     AS work_hour
    FROM "prod_datalake"."view_telemetry_john_deere_machine_measurement_machine_utilization"
    WHERE _name = 'Machine Utilization'
    GROUP BY serial_number, DATE(intervalEndDate)
),

table2 AS (
    SELECT
        e.serialNumber      AS idNumber,
        e._name             AS vehicleName,
        e.fleet_type        AS vehicleType,
        'JOHN DEERE'        AS manufacture,
        mu._date,
        mu.work_hour,
        mu.idle_hour,
        CASE 
            WHEN (mu.work_hour + mu.idle_hour) > 0 THEN 1 
            ELSE 0 
        END AS is_active_day
    FROM latest_equipment e
    LEFT JOIN machine_utilization mu ON e.serialNumber = mu.serialNumber
    WHERE e.rn = 1 AND e._type = 'Machine'
),

-- ============================================
-- LIUGONG
-- ============================================
latest_liugong AS (
    SELECT
        serialNumber,
        productName,
        equipmentModel
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY serialNumber
                   ORDER BY extract_date DESC
               ) AS rn
        FROM "prod_datalake"."view_telemetry_liugong_machine_list"
    ) t
    WHERE rn = 1
),

liugong_daily AS (
    SELECT
        overall_unit_sn     AS serialNumber,
        CAST(extract_date AS DATE) AS _date,
        TRY_CAST(working_hours AS DOUBLE) AS work_hour,
        TRY_CAST(idle_hours AS DOUBLE) AS idle_hour
    FROM "prod_datalake"."view_telemetry_liugong_ilink_work_hours"
),

table3 AS (
    SELECT
        l.serialNumber      AS idNumber,
        l.productName       AS vehicleName,
        l.equipmentModel    AS vehicleType,
        'LIUGONG'           AS manufacture,
        ld._date,
        ld.work_hour,
        ld.idle_hour,
        CASE
            WHEN COALESCE(ld.work_hour, 0) + COALESCE(ld.idle_hour, 0) > 0 THEN 1
            ELSE 0
        END AS is_active_day
    FROM latest_liugong l
    LEFT JOIN liugong_daily ld ON l.serialNumber = ld.serialNumber
),

-- ============================================
-- CATERPILLAR
-- ============================================
latest_caterpillar AS (
    SELECT
        asset_serial_number,
        model
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY asset_serial_number
                   ORDER BY extract_date DESC
               ) AS rn
        FROM "prod_datalake"."view_telemetry_caterpillar_visionlink_assetutilization"
    ) t
    WHERE rn = 1
),

caterpillar_daily AS (
    SELECT
        asset_serial_number     AS serialNumber,
        CAST(extract_date AS DATE) AS _date,
        TRY_CAST(working_time_hours AS DOUBLE) AS work_hour,
        TRY_CAST(idle_time_hours AS DOUBLE) AS idle_hour
    FROM "prod_datalake"."view_telemetry_caterpillar_visionlink_assetutilization"
),

table4 AS (
    SELECT
        l.asset_serial_number   AS idNumber,
        CAST(NULL AS VARCHAR)   AS vehicleName,
        l.model                 AS vehicleType,
        'CATERPILLAR'           AS manufacture,
        cd._date,
        cd.work_hour,
        cd.idle_hour,
        CASE
            WHEN COALESCE(cd.work_hour, 0) + COALESCE(cd.idle_hour, 0) > 0 THEN 1
            ELSE 0
        END AS is_active_day
    FROM latest_caterpillar l
    LEFT JOIN caterpillar_daily cd ON l.asset_serial_number = cd.serialNumber
),

-- ============================================
-- MERGE ALL SOURCES
-- ============================================
merged AS (
    SELECT * FROM table1
    UNION ALL
    SELECT * FROM table2
    UNION ALL
    SELECT * FROM table3
    UNION ALL
    SELECT * FROM table4
)

-- ============================================
-- FINAL OUTPUT
-- ============================================
SELECT
    m.idNumber,
    m.vehicleName,
    m.vehicleType,
    m.manufacture,
    m._date,
    m.work_hour,
    m.idle_hour,
    m.is_active_day,

    CASE
        WHEN (m.work_hour + m.idle_hour) > 0 AND m.is_active_day = 1
        THEN ROUND(m.work_hour / (m.work_hour + m.idle_hour) * 100, 2)
        ELSE NULL
    END AS work_ratio,

    CASE
        WHEN (m.work_hour + m.idle_hour) > 0 AND m.is_active_day = 1
        THEN ROUND(m.idle_hour / (m.work_hour + m.idle_hour) * 100, 2)
        ELSE NULL
    END AS idle_ratio

FROM merged m
ORDER BY m.manufacture, m.idNumber, m._date;
