
put file://Sample1LiftAndShift/target/etl2-0.0.1-FAT.jar @mystage auto_compress=false overwrite=true;

put file://input_inventory.csv @mystage auto_compress=false overwrite=true;


CREATE OR REPLACE PROCEDURE SAMPLE2(ARGS ARRAY)
  RETURNS String
  LANGUAGE JAVA
  RUNTIME_VERSION = '11'
  IMPORTS = ('@mystage/etl2-0.0.1-FAT.jar')
  HANDLER = 'org.example.Sample1LiftAndShift.runProcess'
  PACKAGES = ('com.snowflake:snowpark:latest','com.snowflake:telemetry:latest');

CALL SAMPLE1(['@mystage/input_inventory.csv','@mystage/output_inventory.csv']);

select $1,$2,$3,$4 from @mystage/output_inventory.csv;