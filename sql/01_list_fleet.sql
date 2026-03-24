-- ============================================
-- Query Name : List Fleet
-- Description: Fleet master data integration
-- Author     : Tubagus Nur Rahmat Putra
-- ============================================

WITH
fleet_units AS (
    -- MITSUBISHI
    SELECT
        chassis_number  AS idNumber,
        vehicle_name    AS vehicleName,
        vehicle_type    AS vehicleType,
        'MITSUBISHI'    AS manufacture
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY imei
                   ORDER BY extract_date DESC
               ) AS rn
        FROM "prod_datalake"."view_telemetry_ktbfuso_list_devices"
    ) t
    WHERE rn = 1

    UNION ALL

    -- JOHN DEERE
    SELECT
        serialNumber    AS idNumber,
        _name           AS vehicleName,
        fleet_type      AS vehicleType,
        'JOHN DEERE'    AS manufacture
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY serialNumber
                ORDER BY extract_date DESC
            ) AS rn
        FROM "prod_datalake"."view_telemetry_john_deere_monthly_equipment"
        WHERE telematicsCapable = 'true'
    ) ranked
    WHERE rn = 1 AND _type = 'Machine'

    UNION ALL

    -- LIUGONG
    SELECT
        serialNumber    AS idNumber,
        productName     AS vehicleName,
        equipmentModel  AS vehicleType,
        'LIUGONG'       AS manufacture
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY serialNumber
                   ORDER BY extract_date DESC
               ) AS rn
        FROM "prod_datalake"."view_telemetry_liugong_machine_list"
    ) t
    WHERE rn = 1

    UNION ALL

    -- CATERPILLAR
    SELECT
        concat('CAT', asset_serial_number)  AS idNumber,
        NULL                                AS vehicleName,
        model                               AS vehicleType,
        'CATERPILLAR'                       AS manufacture
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
-- =====================
-- LARK MASTER
-- =====================
lark_fleet AS (
    SELECT
        class,
        equipment_location,
        egi_equipment_description,
        equipment_number        AS vehicleName,
        CASE
            WHEN manufacture = 'CAT' THEN CONCAT(
                'CAT',
                SUBSTR(equipment_serial_number,
                    STRPOS(equipment_serial_number, 'SYW'))
            )
            ELSE equipment_serial_number
        END                     AS idNumber,
        CASE
            WHEN manufacture = 'CAT' THEN 'CATERPILLAR'
            ELSE manufacture
        END                     AS manufacture,
        model,
        number,
        -- CLEANING FIELD USER
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
        END                     AS user
    FROM prod_datalake.view_larksheet_list_of_fleet
    WHERE extract_date = (
        SELECT MAX(extract_date)
        FROM prod_datalake.view_larksheet_list_of_fleet
    )
      AND UPPER(TRIM(manufacture)) IN ('MITSUBISHI', 'JOHN DEERE', 'LIUGONG', 'CAT')
)
SELECT
    lf.equipment_location,
    lf.model,
    lf.class,
    COALESCE(lf.egi_equipment_description, fu.vehicleType)  AS egi_equipment_description,
    lf.user,
    COALESCE(lf.idNumber, fu.idNumber)                      AS idNumber,  -- lark priority
    COALESCE(fu.vehicleName, lf.vehicleName)                AS vehicleName,
    COALESCE(fu.vehicleType, lf.class)                      AS vehicleType,
    COALESCE(fu.manufacture, lf.manufacture)                AS manufacture,
    CASE
        WHEN lf.idNumber IS NULL    THEN 'Datalake Only'
        WHEN fu.idNumber IS NULL    THEN 'Lark Only'
        ELSE 'Matched'
    END                                                     AS data_source
FROM lark_fleet lf
FULL OUTER JOIN fleet_units fu
    ON  UPPER(TRIM(lf.idNumber))    = UPPER(TRIM(fu.idNumber))
    AND UPPER(TRIM(lf.manufacture)) = UPPER(TRIM(fu.manufacture))
ORDER BY
    data_source,
    COALESCE(fu.manufacture, lf.manufacture),
    COALESCE(lf.idNumber, fu.idNumber)
