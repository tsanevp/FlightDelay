REGISTER file:/home/hadoop/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Set number of reducers
SET default_parallel 10;

-- Load Flights1 from CSV file & keep necessary columns
Flights1 = LOAD '$input_path' USING CSVLoader(',');
F1 = FOREACH Flight1 GENERATE
    (int) $0 AS year1,
    (int) $2 AS month1,
    (chararray) $5 AS flightDate1,
    (chararray) $11 AS origin1,
    (chararray) $17 AS dest1,
    ((int) SUBSTRING($35, 0, 2) * 60) + (int) SUBSTRING($35, 2, 4) AS arrTime1,
    (double) $37 AS arrDelayMinutes1,
    (int) $41 AS cancelled1,
    (int) $43 AS diverted1;

-- Load Flights2 from CSV file & keep necessary columns
Flights2 = LOAD '$input_path' USING CSVLoader(',');
F2 = FOREACH Flight2 GENERATE
    (int) $0 AS year2,
    (int) $2 AS month2,
    (chararray) $5 AS flightDate2,
    (chararray) $11 AS origin2,
    (chararray) $17 AS dest2,
    ((int) SUBSTRING($24, 0, 2) * 60) + (int) SUBSTRING($24, 2, 4) AS depTime2,
    (double) $37 AS arrDelayMinutes2,
    (int) $41 AS cancelled2,
    (int) $43 AS diverted2;

-- Filter F1 & F2 by cancelled, diverted, origin, and destination
F1_Filtered_PreJoin = FILTER F1 BY cancelled1 == 0 AND diverted1 == 0 AND (origin1 == "ORD" AND dest1 != "JFK");
F2_Filtered_PreJoin = FILTER F2 BY cancelled2 == 0 AND diverted2 == 0 AND (origin2 != "ORD" AND dest2 == "JFK");

-- Perform the join on conditions: dest1 in F1 matches origin2 in F2, and flight dates match
F1F2 = JOIN F1_Filtered_PreJoin BY (dest1, flightDate1),
                   F2_Filtered_PreJoin BY (origin2, flightDate2);

-- Filter F1F2 join where departure of F2 is after the arrival of F1
F1F2_Dept_Arr_Filtered = FILTER F1F2 BY arrTime1 < depTime2;

-- filter dates not between 06/2007 and 05/2008
F1F2_FlightDate_Filtered = FILTER F1F2_Dept_Arr_Filtered BY 
                            ((year1 == 2007 && month1 >= 6) OR (year1 == 2008 && month1 <= 5)) AND 
                            ((year2 == 2007 && month2 >= 6) OR (year2 == 2008 && month2 <= 5))


-- Calculate the total count and sum
aggregated_data = FOREACH (GROUP F1F2_FlightDate_Filtered ALL) GENERATE 
    COUNT(F1F2_FlightDate_Filtered) AS total_flights,
    SUM(F1F2_FlightDate_Filtered.arrDelayMinutes1 + F1F2_FlightDate_Filtered.arrDelayMinutes2) AS total_delay;

-- Calculate the average
result = FOREACH aggregated_data GENERATE 
    total_flights,
    total_delay,
    (double) total_delay / total_count AS average_delay;

-- Display the result
DUMP result;

-- Store the output
-- STORE filtered_data INTO 'output' USING PigStorage(',');
