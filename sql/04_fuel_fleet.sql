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
-- =====================
-- FUSO: Latest Devices
-- =====================
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
        cast(fuelAverage as integer)  AS fuelLevel,
        cast(odometer as integer)     AS odometer,
        date_trunc('hour', cast(_time as timestamp))
            + INTERVAL '30' MINUTE * (minute(cast(_time as timestamp)) / 30)
        AS window_start
    FROM prod_datalake.view_telemetry_ktbfuso_runner_fuel_reports
    WHERE TRY_CAST(fuelaverage AS double) >= 0
      AND TRY_CAST(fuelaverage AS double) <= 300
      AND TRY_CAST(raw_current_voltage AS double) > 0
),
windowed AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY chassis_number, window_start
                ORDER BY cast(_time as timestamp) DESC
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
        prev_fuel_level,
        fuelLevel,
        CASE
            WHEN prev_fuel_level IS NOT NULL
                 AND fuelLevel < prev_fuel_level
                 AND (prev_fuel_level - fuelLevel) < 50
            THEN (prev_fuel_level - fuelLevel)
            ELSE 0
        END AS fuel_used_liters,
        prev_odometer,
        odometer,
        CASE
            WHEN prev_odometer IS NOT NULL
                 AND odometer > prev_odometer
            THEN (odometer - prev_odometer)
            ELSE 0
        END AS trip_meter
    FROM ordered_data
),
transform_2 AS (
    SELECT
        *,
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
            WHEN ROUND(TRY_CAST(long_drive AS double) / 3600, 2) +
                 ROUND(TRY_CAST(long_idle  AS double) / 3600, 2) > 0
            THEN 1 ELSE 0
        END AS is_active_day
    FROM "prod_datalake"."view_telemetry_ktbfuso_runner_api_daily_summary"
),
fuso_fuel AS (
    SELECT
        f.idNumber,
        d.vehicle_name                                                              AS vehicleName,
        d.vehicle_type                                                              AS vehicleType,
        'MITSUBISHI'                                                                AS manufacture,
        f._date,
        ROUND(SUM(f.idle_fuel_consumption), 2)                                      AS fuel_consumed_idle,
        ROUND(SUM(f.fuel_used_liters), 2) - ROUND(SUM(f.idle_fuel_consumption), 2) AS fuel_consumed_working,
        CAST(NULL AS DOUBLE)                                                        AS fuel_consumed_transport,
        ROUND(SUM(f.fuel_used_liters), 2)                                           AS fuel_consumed_total,
        COALESCE(MAX(fa.is_active_day), 0)                                          AS is_active_day
    FROM transform_2 f
    LEFT JOIN latest_devices d
        ON UPPER(TRIM(f.idNumber)) = UPPER(TRIM(d.chassis_number))
    LEFT JOIN fuso_active fa
        ON d.imei   = fa.imei
        AND f._date = fa._date
    GROUP BY
        f.idNumber,
        d.vehicle_name,
        d.vehicle_type,
        f._date
),

-- =====================
-- JOHN DEERE
-- =====================
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
        serial_number                                                           AS serialNumber,
        DATE(intervalEndDate)                                                   AS _date,
        SUM(idle/3600.0)                                                        AS fuel_consumed_idle,
        SUM(working/3600.0)                                                     AS fuel_consumed_working,
        SUM(transport/3600.0)                                                   AS fuel_consumed_transport,
        SUM(idle/3600.0) + SUM(working/3600.0)                                  AS fuel_consumed_total
    FROM "prod_datalake"."view_telemetry_john_deere_machine_measurement_machine_utilization"
    WHERE _name = 'Fuel Consumed'
    GROUP BY
        serial_number,
        DATE(intervalEndDate)
),
jd_active AS (
    SELECT
        serial_number           AS serialNumber,
        DATE(intervalEndDate)   AS _date,
        CASE
            WHEN SUM(idle + working)/3600.0 > 0 THEN 1 ELSE 0
        END AS is_active_day
    FROM "prod_datalake"."view_telemetry_john_deere_machine_measurement_machine_utilization"
    WHERE _name = 'Machine Utilization'
    GROUP BY
        serial_number,
        DATE(intervalEndDate)
),
jd_fuel AS (
    SELECT
        e.serialNumber                                                          AS idNumber,
        e._name                                                                 AS vehicleName,
        e.fleet_type                                                            AS vehicleType,
        'JOHN DEERE'                                                            AS manufacture,
        f._date,
        f.fuel_consumed_idle,
        f.fuel_consumed_working,
        f.fuel_consumed_transport,
        f.fuel_consumed_total,
        COALESCE(ja.is_active_day, 0)                                           AS is_active_day
    FROM latest_equipment e
    LEFT JOIN fuel_consumed f
        ON e.serialNumber = f.serialNumber
    LEFT JOIN jd_active ja
        ON e.serialNumber = ja.serialNumber
        AND f._date = ja._date
    WHERE e.rn = 1 AND e._type = 'Machine'
),

-- =====================
-- LIUGONG
-- =====================
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
liugong_fuel_daily AS (
    SELECT
        overall_unit_sn                                         AS serialNumber,
        CAST(extract_date AS DATE)                              AS _date,
        TRY_CAST(avg_mileage_idle_oil_consume AS DOUBLE)        AS fuel_consumed_idle,
        TRY_CAST(job_oil_consumes AS DOUBLE)                    AS fuel_consumed_working,
        CAST(NULL AS DOUBLE)                                    AS fuel_consumed_transport,
        CAST(oil_consumes AS DOUBLE)                            AS fuel_consumed_total
    FROM "prod_datalake"."view_telemetry_liugong_ilink_work_hours"
),
liugong_active AS (
    SELECT
        overall_unit_sn                         AS serialNumber,
        CAST(extract_date AS DATE)              AS _date,
        CASE
            WHEN TRY_CAST(working_hours AS DOUBLE) +
                 TRY_CAST(idle_hours AS DOUBLE) > 0 THEN 1 ELSE 0
        END AS is_active_day
    FROM "prod_datalake"."view_telemetry_liugong_ilink_work_hours"
),
liugong_fuel AS (
    SELECT
        l.serialNumber                                                          AS idNumber,
        lg.productName                                                          AS vehicleName,
        lg.equipmentModel                                                       AS vehicleType,
        'LIUGONG'                                                               AS manufacture,
        l._date,
        l.fuel_consumed_idle,
        l.fuel_consumed_working,
        l.fuel_consumed_transport,
        l.fuel_consumed_total,
        COALESCE(la.is_active_day, 0)                                           AS is_active_day
    FROM liugong_fuel_daily l
    LEFT JOIN latest_liugong lg
        ON UPPER(TRIM(l.serialNumber)) = UPPER(TRIM(lg.serialNumber))
    LEFT JOIN liugong_active la
        ON l.serialNumber = la.serialNumber
        AND l._date = la._date
),

-- =====================
-- CATERPILLAR
-- =====================
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
caterpillar_fuel_daily AS (
    SELECT
        concat('CAT', asset_serial_number)          AS serialNumber,
        CAST(extract_date AS DATE)                  AS _date,
        TRY_CAST(working_time_hours AS DOUBLE)      AS working_time_hours,
        TRY_CAST(idle_time_hours AS DOUBLE)         AS idle_time_hours,
        TRY_CAST(total_fuel_burned_liter AS DOUBLE) AS fuel_consumed_total
    FROM "prod_datalake"."view_telemetry_caterpillar_visionlink_assetutilization"
),
caterpillar_fuel AS (
    SELECT
        concat('CAT', lc.asset_serial_number)       AS idNumber,
        CAST(NULL AS VARCHAR)                       AS vehicleName,
        lc.model                                    AS vehicleType,
        'CATERPILLAR'                               AS manufacture,
        f._date,
        ROUND(
            f.fuel_consumed_total *
            f.idle_time_hours /
            NULLIF(f.working_time_hours + f.idle_time_hours, 0)
        , 2)                                        AS fuel_consumed_idle,
        ROUND(
            f.fuel_consumed_total *
            f.working_time_hours /
            NULLIF(f.working_time_hours + f.idle_time_hours, 0)
        , 2)                                        AS fuel_consumed_working,
        CAST(NULL AS DOUBLE)                        AS fuel_consumed_transport,
        f.fuel_consumed_total,
        CASE
            WHEN COALESCE(f.working_time_hours, 0) +
                 COALESCE(f.idle_time_hours, 0) > 0 THEN 1 ELSE 0
        END                                         AS is_active_day
    FROM latest_caterpillar lc
    LEFT JOIN caterpillar_fuel_daily f
        ON concat('CAT', lc.asset_serial_number) = f.serialNumber
),

-- =====================
-- LARK MASTER
-- =====================
lark_fleet AS (
    SELECT
        CASE
            WHEN UPPER(TRIM(manufacture)) = 'CAT'
            THEN CONCAT('CAT', SUBSTR(equipment_serial_number, STRPOS(equipment_serial_number, 'SYW')))
            ELSE UPPER(TRIM(equipment_serial_number))
        END                                                         AS idNumber,
        CASE
            WHEN UPPER(TRIM(manufacture)) = 'CAT' THEN 'CATERPILLAR'
            ELSE UPPER(TRIM(manufacture))
        END                                                         AS manufacture,
        equipment_location,
        egi_equipment_description,
        model,
        class,
        CASE
            WHEN UPPER(TRIM(user)) IN ('NURSERY')           THEN 'NURSERY'
            WHEN UPPER(TRIM(user)) IN ('WAREHOUSE')         THEN 'WAREHOUSE'
            WHEN UPPER(TRIM(user)) IN ('WORKSHOP')          THEN 'WORKSHOP'
            WHEN UPPER(TRIM(user)) IN ('LOGISTIC')          THEN 'LOGISTIC'
            WHEN UPPER(TRIM(user)) IN (
                'AGRI', 'CIVIL', 'EHS', 'FARMING', 'HSE',
                'INFRA', 'LC', 'LD', 'PLANTING', 'R & D'
            )                                               THEN UPPER(TRIM(user))
            WHEN TRIM(user) = '0'                           THEN NULL
            WHEN UPPER(TRIM(user)) = 'BREAKDOWN/ACCIDENT'   THEN NULL
            WHEN TRIM(user) = 'VACANT'                      THEN 'VACANT'
            ELSE NULL
        END                                                         AS user_company
    FROM prod_datalake.view_larksheet_list_of_fleet
    WHERE extract_date = (
        SELECT MAX(extract_date)
        FROM prod_datalake.view_larksheet_list_of_fleet
    )
      AND UPPER(TRIM(manufacture)) IN ('MITSUBISHI', 'JOHN DEERE', 'LIUGONG', 'CAT')
),

-- =====================
-- MERGE ALL
-- =====================
merged AS (
    SELECT * FROM fuso_fuel
    UNION ALL
    SELECT * FROM jd_fuel
    UNION ALL
    SELECT * FROM liugong_fuel
    UNION ALL
    SELECT * FROM caterpillar_fuel
),

-- =====================
-- JOIN KE LARK (hasilkan model & class per idNumber)
-- =====================
joined AS (
    SELECT
        m.idNumber,
        m.vehicleName,
        m.vehicleType,
        m.manufacture,
        m._date,
        m.fuel_consumed_idle,
        m.fuel_consumed_working,
        m.fuel_consumed_transport,
        m.fuel_consumed_total,
        m.is_active_day,
        lf.equipment_location,
        lf.egi_equipment_description,
        lf.model,
        lf.class,
        lf.user_company
    FROM merged m
    LEFT JOIN lark_fleet lf
        ON  UPPER(TRIM(m.idNumber))    = lf.idNumber
        AND UPPER(TRIM(m.manufacture)) = lf.manufacture
),

-- =====================
-- MASTER: model & class per vehicleType + manufacture
-- (mengambil nilai non-null yang sudah berhasil join ke lark)
-- =====================
vehicle_master AS (
    SELECT
        manufacture,
        vehicleType,
        MAX(model) AS model,
        MAX(class) AS class,
        MAX(egi_equipment_description) AS egi_equipment_description
    FROM joined
    WHERE model IS NOT NULL
      AND class IS NOT NULL
    GROUP BY manufacture, vehicleType
)

-- =====================
-- FINAL OUTPUT
-- =====================
SELECT
    j.idNumber,
    j.vehicleName,
    j.vehicleType,
    j.manufacture,
    j._date,
    j.fuel_consumed_idle,
    j.fuel_consumed_working,
    j.fuel_consumed_transport,
    j.fuel_consumed_total,
    j.is_active_day,
    j.equipment_location,
    COALESCE(j.model, vm.model) AS model,
    COALESCE(j.class, vm.class) AS class,
    COALESCE(j.egi_equipment_description, vm.egi_equipment_description) AS equipment_description, 
    j.user_company
FROM joined j
LEFT JOIN vehicle_master vm
    ON  UPPER(TRIM(j.manufacture)) = UPPER(TRIM(vm.manufacture))
    AND UPPER(TRIM(j.vehicleType)) = UPPER(TRIM(vm.vehicleType))
ORDER BY j.manufacture, j.idNumber, j._date
