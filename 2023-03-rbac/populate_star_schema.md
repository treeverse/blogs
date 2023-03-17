## `dim_asset`

```sql
CREATE SEQUENCE seq_assetid START 1;

CREATE TABLE dim_asset (
asset_id integer primary key default nextval('seq_assetid'),
type varchar,
model varchar,
rid_equipped boolean);
```

```sql
INSERT INTO dim_asset (type, model, rid_equipped)
WITH X AS (SELECT "Asset Type" AS TYPE, "Asset Model" AS MODEL, "RID Equipped" as rid_equipped
			 FROM Registations_P107_Active 
		   UNION ALL
		   SELECT "UAS Type" AS TYPE, "UAS Model" AS MODEL, "RID Equipped" as rid_equipped
		     FROM Registations_RecFlyer_Active)
SELECT DISTINCT TYPE, MODEL, rid_equipped FROM X;
```

```sql
D SELECT COUNT(*) FROM dim_asset;
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│        43015 │
└──────────────┘

D SELECT * FROM dim_asset LIMIT 5;
┌──────────┬─────────────────┬────────────────────┬──────────────┐
│ asset_id │      type       │       model        │ rid_equipped │
│  int32   │     varchar     │      varchar       │   boolean    │
├──────────┼─────────────────┼────────────────────┼──────────────┤
│        1 │ HOMEBUILT_UAS   │ Drak               │ false        │
│        2 │ TRADITIONAL_UAS │ Inspire 2          │ false        │
│        3 │ TRADITIONAL_UAS │ Phantom 3 Standard │ false        │
│        4 │ TRADITIONAL_UAS │ PHANTOM 4          │ false        │
│        5 │ TRADITIONAL_UAS │ Alta 8             │ false        │
└──────────┴─────────────────┴────────────────────┴──────────────┘
```

## `dim_location`

```sql
CREATE SEQUENCE seq_locationid START 1;

CREATE TABLE dim_location (
location_id integer primary key default nextval('seq_locationid'),
CITY VARCHAR, 
STATE_PROVINCE VARCHAR, 
POSTAL_CODE VARCHAR);
```

```SQL
INSERT INTO dim_location (CITY, STATE_PROVINCE, POSTAL_CODE)
WITH X AS (SELECT  "Physical City" as CITY,
                    "Physical State/Province" as STATE_PROVINCE,
                    "Physical Postal Code" as POSTAL_CODE
            FROM Registations_P107_Active 
            UNION ALL
            SELECT  "Mailing City" as CITY,
                    "Mailing State/Province" as STATE_PROVINCE,
                    "Mailing Postal Code" as POSTAL_CODE
            FROM Registations_RecFlyer_Active
            )
SELECT DISTINCT CITY, STATE_PROVINCE, POSTAL_CODE FROM X;
```

```sql
D SELECT COUNT(*) FROM dim_location;
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│        71136 │
└──────────────┘

D SELECT * FROM dim_location LIMIT 5;
┌─────────────┬───────────────┬────────────────┬─────────────┐
│ location_id │     CITY      │ STATE_PROVINCE │ POSTAL_CODE │
│    int32    │    varchar    │    varchar     │   varchar   │
├─────────────┼───────────────┼────────────────┼─────────────┤
│           1 │ HUACHUCA CITY │ AZ             │ 85616       │
│           2 │ Fort Morgan   │ CO             │ 80701       │
│           3 │ Manvel        │ TX             │ 77578       │
│           4 │ Tupelo        │ MS             │ 38801       │
│           5 │ Tampa         │ FL             │ 33609       │
└─────────────┴───────────────┴────────────────┴─────────────┘
```

## `fact_registrations`

```sql
CREATE OR REPLACE TABLE fact_registrations AS 
SELECT "Registration Date" as date_of_reg,
        "Registion Expire Dt" as reg_expiry_date,
        'P107' as reg_class,
        asset_id,
        lp.location_id as physical_location_id,
        lm.location_id as mailing_location_id
    from Registations_P107_Active r
         left outer join dim_asset a on r."Asset Type" = a.type
                                    and r."RID Equipped" = a.rid_equipped
                                    and r."Asset Model" = a.model
        left outer join dim_location lp on "Physical City" = lp.City
                                       and "Physical State/Province" = lp.State_Province
                                       and "Physical Postal Code" = lp.Postal_Code
        left outer join dim_location lm on "Mailing City" = lm.City
                                       and "Mailing State/Province" = lm.State_Province
                                       and "Mailing Postal Code" = lm.Postal_Code
UNION ALL
SELECT "Registration Date" as date_of_reg,
        "Registion Expire Dt" as reg_expiry_date,
        'RecFlyer' as reg_class,
        asset_id,
        lp.location_id as physical_location_id,
        lm.location_id as mailing_location_id
    from Registations_RecFlyer_Active r
         left outer join dim_asset a on r."UAS Type" = a.type
                                    and r."RID Equipped" = a.rid_equipped
                                    and r."UAS Model" = a.model
        left outer join dim_location lp on "Physical City" = lp.City
                                       and "Physical State/Province" = lp.State_Province
                                       and "Physical Postal Code" = lp.Postal_Code
        left outer join dim_location lm on "Mailing City" = lm.City
                                       and "Mailing State/Province" = lm.State_Province
                                       and "Mailing Postal Code" = lm.Postal_Code;
```

```sql
D SELECT COUNT(*) FROM fact_registrations;
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│       875000 │
└──────────────┘
D DESCRIBE fact_registrations;
┌──────────────────────┬─────────────┬─────────┬─────────┬─────────┬───────┐
│     column_name      │ column_type │  null   │   key   │ default │ extra │
│       varchar        │   varchar   │ varchar │ varchar │ varchar │ int32 │
├──────────────────────┼─────────────┼─────────┼─────────┼─────────┼───────┤
│ date_of_reg          │ VARCHAR     │ YES     │         │         │       │
│ reg_expiry_date      │ VARCHAR     │ YES     │         │         │       │
│ reg_class            │ VARCHAR     │ YES     │         │         │       │
│ asset_id             │ INTEGER     │ YES     │         │         │       │
│ physical_location_id │ INTEGER     │ YES     │         │         │       │
│ mailing_location_id  │ INTEGER     │ YES     │         │         │       │
└──────────────────────┴─────────────┴─────────┴─────────┴─────────┴───────┘
```
