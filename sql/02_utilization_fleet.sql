-- ============================================
-- Query Name : Utilization Fleet
-- File Name  : 03_utilization_fleet.sql
-- Author     : Tubagus Nur Rahmat Putra
-- Description:
--   This query calculates fleet utilization based on
--   active operating days within the last 30 days.
--
-- Key Metrics:
--   - total_unit
--   - total_underutilized_unit
--   - total_utilized_unit
--
-- ============================================

WITH
latest_devices AS (
    SELECT
        imei,
        chassis_number,
        vehicle_name,
        vehicle_type,
        CAST(NULLIF(activation_date, '') AS DATE) AS activation_date
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
fuso_daily AS (
    SELECT
        imei,
        CAST(SUBSTR(_date, 1, 10) AS DATE) AS _date,
        ROUND(TRY_CAST(long_drive AS double) / 3600, 2) +
        ROUND(TRY_CAST(long_idle AS double) / 3600, 2) AS work_hour
    FROM "prod_datalake"."view_telemetry_ktbfuso_runner_api_daily_summary"
    WHERE CAST(SUBSTR(_date, 1, 10) AS DATE) >= CURRENT_DATE - INTERVAL '30' DAY
),
fuso_unit_active AS (
    SELECT
        d.chassis_number        AS idNumber,
        d.vehicle_name          AS vehicleName,
        d.vehicle_type          AS vehicleType,
        'MITSUBISHI'            AS manufacture,
        SUM(CASE WHEN ds.work_hour > 0 THEN 1 ELSE 0 END) AS total_active_days
    FROM latest_devices d
    LEFT JOIN fuso_daily ds ON d.imei = ds.imei
    WHERE d.chassis_number IS NOT NULL
      AND TRIM(d.chassis_number) != ''
    GROUP BY
        d.chassis_number,
        d.vehicle_name,
        d.vehicle_type
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
jd_daily AS (
    SELECT
        serial_number               AS serialNumber,
        DATE(intervalEndDate)       AS _date,
        (idle/3600.0) + (working/3600.0) AS work_hour
    FROM "prod_datalake"."view_telemetry_john_deere_machine_measurement_machine_utilization"
    WHERE _name = 'Machine Utilization'
      AND DATE(intervalEndDate) >= CURRENT_DATE - INTERVAL '30' DAY
),
jd_unit_active AS (
    SELECT
        e.serialNumber              AS idNumber,
        e._name                     AS vehicleName,
        e.fleet_type                AS vehicleType,
        'JOHN DEERE'                AS manufacture,
        SUM(CASE WHEN mu.work_hour > 0 THEN 1 ELSE 0 END) AS total_active_days
    FROM latest_equipment e
    LEFT JOIN jd_daily mu ON e.serialNumber = mu.serialNumber
    WHERE e.rn = 1 AND e._type = 'Machine'
      AND e.serialNumber IS NOT NULL
      AND TRIM(e.serialNumber) != ''
    GROUP BY
        e.serialNumber,
        e._name,
        e.fleet_type
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
        overall_unit_sn             AS serialNumber,
        CAST(extract_date AS DATE)  AS _date,
        TRY_CAST(working_hours AS DOUBLE) +
        TRY_CAST(idle_hours AS DOUBLE)    AS work_hour
    FROM "prod_datalake"."view_telemetry_liugong_ilink_work_hours"
    WHERE CAST(extract_date AS DATE) >= CURRENT_DATE - INTERVAL '30' DAY
),
liugong_unit_active AS (
    SELECT
        l.serialNumber              AS idNumber,
        l.productName               AS vehicleName,
        l.equipmentModel            AS vehicleType,
        'LIUGONG'                   AS manufacture,
        SUM(CASE WHEN ld.work_hour > 0 THEN 1 ELSE 0 END) AS total_active_days
    FROM latest_liugong l
    LEFT JOIN liugong_daily ld ON l.serialNumber = ld.serialNumber
    WHERE l.serialNumber IS NOT NULL
      AND TRIM(l.serialNumber) != ''
    GROUP BY
        l.serialNumber,
        l.productName,
        l.equipmentModel
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
        asset_serial_number         AS serialNumber,
        CAST(extract_date AS DATE)  AS _date,
        TRY_CAST(working_time_hours AS DOUBLE) +
        TRY_CAST(idle_time_hours AS DOUBLE)    AS work_hour
    FROM "prod_datalake"."view_telemetry_caterpillar_visionlink_assetutilization"
    WHERE CAST(extract_date AS DATE) >= CURRENT_DATE - INTERVAL '30' DAY
),
caterpillar_unit_active AS (
    SELECT
        l.asset_serial_number       AS idNumber,
        CAST(NULL AS VARCHAR)       AS vehicleName,
        l.model                     AS vehicleType,
        'CATERPILLAR'               AS manufacture,
        SUM(CASE WHEN cd.work_hour > 0 THEN 1 ELSE 0 END) AS total_active_days
    FROM latest_caterpillar l
    LEFT JOIN caterpillar_daily cd ON l.asset_serial_number = cd.serialNumber
    WHERE l.asset_serial_number IS NOT NULL
      AND TRIM(l.asset_serial_number) != ''
    GROUP BY
        l.asset_serial_number,
        l.model
),
all_units AS (
    SELECT * FROM fuso_unit_active
    UNION ALL
    SELECT * FROM jd_unit_active
    UNION ALL
    SELECT * FROM liugong_unit_active
    UNION ALL
    SELECT * FROM caterpillar_unit_active
),
lark_fleet AS (
    SELECT
        CASE
            WHEN manufacture = 'CAT' THEN CONCAT('CAT', SUBSTR(equipment_serial_number, STRPOS(equipment_serial_number, 'SYW')))
            ELSE equipment_serial_number
        END                                                         AS idNumber,
        CASE WHEN manufacture = 'CAT' THEN 'CATERPILLAR' ELSE manufacture END AS manufacture,
        equipment_location,
        egi_equipment_description,
        model,
        class,
        user                                                        AS user_company
    FROM prod_datalake.view_larksheet_list_of_fleet
    WHERE extract_date = (
        SELECT MAX(extract_date)
        FROM prod_datalake.view_larksheet_list_of_fleet
    )
      AND UPPER(TRIM(manufacture)) IN ('MITSUBISHI', 'JOHN DEERE', 'LIUGONG', 'CAT')
),

-- =====================
-- JOIN ALL UNITS + LARK
-- =====================
joined AS (
    SELECT
        UPPER(TRIM(a.manufacture))          AS manufacture,
        UPPER(TRIM(a.vehicleType))          AS vehicleType,
        lf.equipment_location,
        lf.egi_equipment_description,       -- murni dari lark, fallback lewat master
        lf.model,
        lf.class,
        lf.user_company,
        a.total_active_days
    FROM all_units a
    LEFT JOIN lark_fleet lf
        ON  UPPER(TRIM(a.idNumber))    = lf.idNumber
        AND UPPER(TRIM(a.manufacture)) = lf.manufacture
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
    j.manufacture,
    j.vehicleType,
    j.equipment_location,
    COALESCE(j.egi_equipment_description, vm.egi_equipment_description) AS equipment_description,
    COALESCE(j.model, vm.model)                                         AS model,
    COALESCE(j.class, vm.class)                                         AS class,
    j.user_company,
    COUNT(*)                                                            AS total_unit,
    SUM(CASE WHEN j.total_active_days < 10 THEN 1 ELSE 0 END)          AS total_underutilized_unit,
    COUNT(*) - SUM(CASE WHEN j.total_active_days < 10 THEN 1 ELSE 0 END) AS total_utilized_unit
FROM joined j
LEFT JOIN vehicle_master vm
    ON  j.manufacture = vm.manufacture
    AND j.vehicleType = vm.vehicleType
GROUP BY
    j.manufacture,
    j.vehicleType,
    j.equipment_location,
    COALESCE(j.egi_equipment_description, vm.egi_equipment_description),
    COALESCE(j.model, vm.model),
    COALESCE(j.class, vm.class),
    j.user_company
ORDER BY
    j.manufacture,
    total_underutilized_unit DESC
