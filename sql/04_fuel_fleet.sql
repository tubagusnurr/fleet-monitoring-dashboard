-- ============================================
-- Query Name : Fuel Fleet
-- File Name  : 02_fuel_fleet.sql
-- Author     : Tubagus Nur Rahmat Putra
-- Description:
--   This query calculates fuel consumption per fleet unit
--   by separating idle, working, and total fuel usage.
--
--   It integrates multiple telemetry sources and applies
--   transformation logic to estimate fuel usage accurately.
--
-- Key Metrics:
--   - fuel_consumed_idle
--   - fuel_consumed_working
--   - fuel_consumed_total
--   - is_active_day
-- ============================================

WITH

-- ============================================
-- MITSUBISHI (FUSO)
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

fuel_reports AS (
    SELECT
        extract_date,
        chassis_number,
        _time,
        CAST(fuelAverage AS INTEGER)  AS fuelLevel,
        CAST(odometer AS INTEGER)     AS odometer,
        date_trunc('hour', CAST(_time AS TIMESTAMP))
        + INTERVAL '30' MINUTE * (minute(CAST(_time AS TIMESTAMP)) / 30)
        AS window_start
    FROM prod_datalake.view_telemetry_ktbfuso_runner_fuel_reports
    WHERE TRY_CAST(fuelAverage AS DOUBLE) BETWEEN 0 AND 300
      AND TRY_CAST(raw_current_voltage AS DOUBLE) > 0
),

windowed AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY chassis_number, window_start
                   ORDER BY CAST(_time AS TIMESTAMP) DESC
               ) AS rn
        FROM fuel_reports
    )
    WHERE rn = 1
),

ordered_data AS (
    SELECT
        extract_date,
        chassis_number,
        window_start,
        _time,
        fuelLevel,
        LAG(fuelLevel) OVER (
            PARTITION BY chassis_number
            ORDER BY window_start
        ) AS prev_fuel_level,
        odometer,
        LAG(odometer) OVER (
            PARTITION BY chassis_number
            ORDER BY window_start
        ) AS prev_odometer
    FROM windowed
),

transform_1 AS (
    SELECT
        extract_date   AS _date,
        chassis_number AS idNumber,
        window_start,
        _time,
        fuelLevel,
        prev_fuel_level,
        CASE
            WHEN prev_fuel_level IS NOT NULL
                 AND fuelLevel < prev_fuel_level
                 AND (prev_fuel_level - fuelLevel) < 50
            THEN (prev_fuel_level - fuelLevel)
            ELSE 0
        END AS fuel_used_liters,

        odometer,
        prev_odometer,
        CASE
            WHEN prev_odometer IS NOT NULL
                 AND odometer > prev_odometer
            THEN (odometer - prev_odometer)
            ELSE 0
        END AS trip_meter
    FROM ordered_data
),

transform_2 AS (
    SELECT *,
        CASE
            WHEN fuel_used_liters > 0 AND trip_meter = 0
            THEN fuel_used_liters
            ELSE 0
        END AS idle_fuel_consumption
    FROM transform_1
),

fuso_active AS (
    SELECT
        imei,
        CAST(SUBSTR(_date, 1, 10) AS DATE) AS _date,
        CASE
            WHEN ROUND(TRY_CAST(long_drive AS DOUBLE) / 3600, 2) +
                 ROUND(TRY_CAST(long_idle AS DOUBLE) / 3600, 2) > 0
            THEN 1 ELSE 0
        END AS is_active_day
    FROM "prod_datalake"."view_telemetry_ktbfuso_runner_api_daily_summary"
),

fuso_fuel AS (
    SELECT
        f.idNumber,
        d.vehicle_name        AS vehicleName,
        d.vehicle_type        AS vehicleType,
        'MITSUBISHI'          AS manufacture,
        f._date,

        ROUND(SUM(f.idle_fuel_consumption), 2) AS fuel_consumed_idle,
        ROUND(SUM(f.fuel_used_liters), 2)
            - ROUND(SUM(f.idle_fuel_consumption), 2) AS fuel_consumed_working,
        CAST(NULL AS DOUBLE) AS fuel_consumed_transport,
        ROUND(SUM(f.fuel_used_liters), 2) AS fuel_consumed_total,

        COALESCE(MAX(fa.is_active_day), 0) AS is_active_day
    FROM transform_2 f
    LEFT JOIN latest_devices d
        ON UPPER(TRIM(f.idNumber)) = UPPER(TRIM(d.chassis_number))
    LEFT JOIN fuso_active fa
        ON d.imei = fa.imei AND f._date = fa._date
    GROUP BY f.idNumber, d.vehicle_name, d.vehicle_type, f._date
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

fuel_consumed AS (
    SELECT
        serial_number AS serialNumber,
        DATE(intervalEndDate) AS _date,
        SUM(idle/3600.0)      AS fuel_consumed_idle,
        SUM(working/3600.0)   AS fuel_consumed_working,
        SUM(transport/3600.0) AS fuel_consumed_transport,
        SUM(idle/3600.0) + SUM(working/3600.0) AS fuel_consumed_total
    FROM "prod_datalake"."view_telemetry_john_deere_machine_measurement_machine_utilization"
    WHERE _name = 'Fuel Consumed'
    GROUP BY serial_number, DATE(intervalEndDate)
),

jd_active AS (
    SELECT
        serial_number AS serialNumber,
        DATE(intervalEndDate) AS _date,
        CASE 
            WHEN SUM(idle + working)/3600.0 > 0 THEN 1 ELSE 0 
        END AS is_active_day
    FROM "prod_datalake"."view_telemetry_john_deere_machine_measurement_machine_utilization"
    WHERE _name = 'Machine Utilization'
    GROUP BY serial_number, DATE(intervalEndDate)
),

jd_fuel AS (
    SELECT
        e.serialNumber,
        e._name,
        e.fleet_type,
        'JOHN DEERE' AS manufacture,
        f._date,
        f.fuel_consumed_idle,
        f.fuel_consumed_working,
        f.fuel_consumed_transport,
        f.fuel_consumed_total,
        COALESCE(ja.is_active_day, 0) AS is_active_day
    FROM latest_equipment e
    LEFT JOIN fuel_consumed f ON e.serialNumber = f.serialNumber
    LEFT JOIN jd_active ja ON e.serialNumber = ja.serialNumber AND f._date = ja._date
    WHERE e.rn = 1 AND e._type = 'Machine'
),

-- ============================================
-- MERGE ALL
-- ============================================

merged AS (
    SELECT * FROM fuso_fuel
    UNION ALL
    SELECT * FROM jd_fuel
)

SELECT *
FROM merged
ORDER BY manufacture, idNumber, _date;
