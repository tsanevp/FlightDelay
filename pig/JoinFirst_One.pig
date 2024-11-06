REGISTER file:/home/hadoop/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Set number of reducers
SET default_parallel 10;

-- Load a CSV file
f1 = LOAD '$input_path' USING CSVLoader(',');

f2 = LOAD '$input_path' USING CSVLoader(',');

-- Filter rows where field2 is greater than 10
filtered_data = FILTER data BY field2 > 10;

-- Store the output
STORE filtered_data INTO 'output' USING PigStorage(',');
