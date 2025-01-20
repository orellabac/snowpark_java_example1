

put file://Sample1_WithTables/target/etl3-0.0.1-FAT.jar @mystage auto_compress=false overwrite=true;

CREATE OR REPLACE FILE FORMAT mycsv
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

CREATE OR REPLACE TABLE INPUT_INVENTORY (
    Day VARCHAR(20) NOT NULL,
    Material INT NOT NULL,
    Labor INT NOT NULL
);

CREATE OR REPLACE TABLE OPTIMIZED_PRODUCTION (
    DAY VARCHAR,
    OPTIMAL_A FLOAT,
    OPTIONAL_B FLOAT,
    MAXPROFIT FLOAT
);

INSERT INTO INPUT_INVENTORY (Day, Material, Labor) VALUES ('Monday', 100, 80);
INSERT INTO INPUT_INVENTORY (Day, Material, Labor) VALUES ('Tuesday', 150, 120);
INSERT INTO INPUT_INVENTORY (Day, Material, Labor) VALUES ('Wednesday', 90, 100);
INSERT INTO INPUT_INVENTORY (Day, Material, Labor) VALUES ('Thursday', 120, 110);
INSERT INTO INPUT_INVENTORY (Day, Material, Labor) VALUES ('Friday', 110, 90);



CREATE OR REPLACE PROCEDURE SAMPLE2(ARGS ARRAY)
  RETURNS String
  LANGUAGE JAVA
  RUNTIME_VERSION = '11'
  IMPORTS = ('@mystage/etl3-0.0.1-FAT.jar')
  HANDLER = 'org.example.Sample1_WithTables.runProcess'
  PACKAGES = ('com.snowflake:snowpark:latest','com.snowflake:telemetry:latest');

CALL SAMPLE2(['INPUT_INVENTORY','OPTIMIZED_PRODUCTION']);

SELECT * FROM OPTIMIZED_PRODUCTION;