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
latest_devices AS (
    SELECT 
        extract_date,
        imei,
        chassis_number,
        vehicle_number,
        vehicle_name,
        vehicle_type,
        CAST(NULLIF(activation_date, '') AS DATE) AS activation_date, 
        gsm_number,
        runner_gpstype,
        additional_data,
        owner_id
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
daily_summary AS (
    SELECT 
        CAST(SUBSTR(_date, 1, 10) AS DATE) AS date_record,
        ROUND(TRY_CAST(long_drive AS double) / 3600, 2) AS long_drive,
        ROUND(TRY_CAST(long_idle AS double) / 3600, 2) AS long_idle,
        ROUND(TRY_CAST(distance AS double), 2) AS distance,
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
            WHEN ROUND((ds.long_drive + ds.long_idle), 2) > 0 THEN 1 
            ELSE 0 
        END AS is_active_day
    FROM latest_devices d
    LEFT JOIN daily_summary ds ON d.imei = ds.imei
),
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
        SUM(idle/3600.0)        AS machine_utilization_idle,
        SUM(working/3600.0)     AS machine_utilization_working
    FROM "prod_datalake"."view_telemetry_john_deere_machine_measurement_machine_utilization"
    WHERE _name = 'Machine Utilization'
    GROUP BY
        serial_number,
        DATE(intervalEndDate)
),
table2 AS (
    SELECT
        e.serialNumber              AS idNumber,
        e._name                     AS vehicleName,
        e.fleet_type                AS vehicleType,
        'JOHN DEERE'                AS manufacture,
        mu._date                    AS _date,
        mu.machine_utilization_working AS work_hour,
        mu.machine_utilization_idle    AS idle_hour,
        CASE 
            WHEN ROUND((mu.machine_utilization_working + mu.machine_utilization_idle), 4) > 0 THEN 1 
            ELSE 0 
        END AS is_active_day
    FROM latest_equipment e
    LEFT JOIN machine_utilization mu ON e.serialNumber = mu.serialNumber
    WHERE e.rn = 1 AND e._type = 'Machine'
),
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
        overall_unit_sn                         AS serialNumber,
        CAST(extract_date AS DATE)              AS _date,
        TRY_CAST(working_hours AS DOUBLE)       AS work_hour,
        TRY_CAST(idle_hours AS DOUBLE)          AS idle_hour
    FROM "prod_datalake"."view_telemetry_liugong_ilink_work_hours"
),
table3 AS (
    SELECT
        l.serialNumber              AS idNumber,
        l.productName               AS vehicleName,
        l.equipmentModel            AS vehicleType,
        'LIUGONG'                   AS manufacture,
        ld._date                    AS _date,
        ld.work_hour                AS work_hour,
        ld.idle_hour                AS idle_hour,
        CASE
            WHEN COALESCE(ld.work_hour, 0) + COALESCE(ld.idle_hour, 0) > 0 THEN 1
            ELSE 0
        END AS is_active_day
    FROM latest_liugong l
    LEFT JOIN liugong_daily ld ON l.serialNumber = ld.serialNumber
),
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
        asset_serial_number                             AS serialNumber,
        CAST(extract_date AS DATE)                      AS _date,
        TRY_CAST(working_time_hours AS DOUBLE)          AS work_hour,
        TRY_CAST(idle_time_hours AS DOUBLE)             AS idle_hour
    FROM "prod_datalake"."view_telemetry_caterpillar_visionlink_assetutilization"
),
table4 AS (
    SELECT
        l.asset_serial_number           AS idNumber,
        CAST(NULL AS VARCHAR)           AS vehicleName,
        l.model                         AS vehicleType,
        'CATERPILLAR'                   AS manufacture,
        cd._date                        AS _date,
        cd.work_hour                    AS work_hour,
        cd.idle_hour                    AS idle_hour,
        CASE
            WHEN COALESCE(cd.work_hour, 0) + COALESCE(cd.idle_hour, 0) > 0 THEN 1
            ELSE 0
        END AS is_active_day
    FROM latest_caterpillar l
    LEFT JOIN caterpillar_daily cd ON l.asset_serial_number = cd.serialNumber
),
lark_fleet AS (
    SELECT
        CASE 
            WHEN manufacture = 'CAT' THEN CONCAT('CAT', SUBSTR(equipment_serial_number, STRPOS(equipment_serial_number, 'SYW')))
            ELSE UPPER(TRIM(equipment_serial_number))
        END                                             AS idNumber,
        CASE 
            WHEN UPPER(TRIM(manufacture)) = 'CAT' THEN 'CATERPILLAR'
            ELSE UPPER(TRIM(manufacture))
        END                                             AS manufacture,
        equipment_location,
        egi_equipment_description,                      -- murni dari lark, fallback lewat master
        model,
        class
    FROM prod_datalake.view_larksheet_list_of_fleet
    WHERE extract_date = (
        SELECT MAX(extract_date) 
        FROM prod_datalake.view_larksheet_list_of_fleet
    )
      AND UPPER(TRIM(manufacture)) IN ('MITSUBISHI', 'JOHN DEERE', 'LIUGONG', 'CAT')
),
merged AS (
    SELECT * FROM table1
    UNION ALL
    SELECT * FROM table2
    UNION ALL
    SELECT * FROM table3
    UNION ALL
    SELECT * FROM table4
),

-- =====================
-- JOIN MERGED + LARK
-- =====================
joined AS (
    SELECT
        m.idNumber,
        m.vehicleName,
        m.vehicleType,
        m.manufacture,
        m._date,
        m.work_hour,
        m.idle_hour,
        m.is_active_day,
        lf.equipment_location,
        lf.egi_equipment_description,       -- murni dari lark
        lf.model,
        lf.class
    FROM merged m
    LEFT JOIN lark_fleet lf
        ON  UPPER(TRIM(m.idNumber))    = lf.idNumber
        AND UPPER(TRIM(m.manufacture)) = lf.manufacture
),

-- =====================
-- MASTER: model, class, egi_equipment_description per vehicleType + manufacture
-- =====================
vehicle_master AS (
    SELECT
        manufacture,
        vehicleType,
        MAX(model)                      AS model,
        MAX(class)                      AS class,
        MAX(egi_equipment_description)  AS egi_equipment_description
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
    j.work_hour,
    j.idle_hour,
    j.is_active_day,
    j.equipment_location,
    COALESCE(j.egi_equipment_description, vm.egi_equipment_description) AS equipment_description,
    COALESCE(j.model, vm.model)                                         AS model,
    COALESCE(j.class, vm.class)                                         AS class,
    CASE
        WHEN ROUND(j.work_hour + j.idle_hour, 4) > 0 AND j.is_active_day = 1
        THEN ROUND(j.work_hour / (j.work_hour + j.idle_hour) * 100, 2)
        ELSE NULL
    END AS work_ratio,
    CASE
        WHEN ROUND(j.work_hour + j.idle_hour, 4) > 0 AND j.is_active_day = 1
        THEN ROUND(j.idle_hour / (j.work_hour + j.idle_hour) * 100, 2)
        ELSE NULL
    END AS idle_ratio
FROM joined j
LEFT JOIN vehicle_master vm
    ON  j.manufacture = vm.manufacture
    AND j.vehicleType = vm.vehicleType
ORDER BY j.manufacture, j.idNumber, j._date
