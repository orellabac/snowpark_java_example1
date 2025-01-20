# Snowpark Java Examples 1

This is probably the first some some groups of examples to introduce some concepts about using Java with Snowpark Java.

This repository has the following structure:

```
+ Sample1
+ Sample1LiftAndShift
+ Sample1With_Tables
+ Sample1UTDF 
```

## Sample1

This is the initial java sample. We use it to provide a very simplistic java program. The idea is to walk you thru the process of running code with Snowpark Java. So the other projects will use this as a base.

## Sample1LiftAndShift

This project takes the code from Sample1 and tries to make minimal changes to run in Snowflake. For example the original sample read data from a csv and writes results to a csv. This version does the same using an stage. A more natural approach for Snowflake will be to read data from tables but that is explored in the next example.

## Sample1With_Tables

This project takes the code from Sample1LiftAndShift and makes minimal changes so the input and output are read and written to snowflake tables.

## Sample1UTDF

Takes the code from Sample1With_Tables and modifies it to turn it into a tabular function, to provide an introduction to other methods of interation in snowpark java.

Building the projects

For all the projects there is a maven pom.xml, just open a terminal into that folder and run:

`mvn clean package`
