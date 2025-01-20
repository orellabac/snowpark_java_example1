
put file://Sample1_UDTF/target/etl4-0.0.1-FAT.jar @mystage auto_compress=false overwrite=true;


CREATE OR REPLACE FUNCTION SAMPLE_UDTF(DAY STRING, MATERIAL Float, LABOR Float)
  RETURNS TABLE (DAY STRING, OPTIMAL_A Float, OPTIMAL_B Float, MAXPROFIT Float)
  LANGUAGE JAVA
  RUNTIME_VERSION = '11'
  IMPORTS = ('@mystage/etl4-0.0.1-FAT.jar')
  HANDLER = 'org.example.Sample1_UDTF'
  PACKAGES = ('com.snowflake:snowpark:latest','com.snowflake:telemetry:latest');

SELECT INPUT.*, RESULTS.* FROM INPUT_INVENTORY AS INPUT,
TABLE(SAMPLE_UDTF(INPUT.Day, INPUT.Material::Float, INPUT.Labor::Float)) AS RESULTS;