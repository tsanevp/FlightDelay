REGISTER s3://a3b/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Set number of reducers
SET default_parallel 10;

-- Load Flights1 from CSV file & keep necessary columns
Flights1 = LOAD '$INPUT' USING CSVLoader(',');
Flight1 = FOREACH Flights1 GENERATE
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
Flights2 = LOAD '$INPUT' USING CSVLoader(',');
Flight2 = FOREACH Flights2 GENERATE
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
F1_Filtered = FILTER Filtered_Flight1 BY (cancelled1 == 0 AND diverted1 == 0 AND origin1 == 'ORD' AND dest1 != 'JFK');
F2_Filtered = FILTER Filtered_Flight2 BY (cancelled2 == 0 AND diverted2 == 0 AND origin2 != 'ORD' AND dest2 == 'JFK');

-- Perform the join on conditions: dest1 in F1 matches origin2 in F2, and flight dates match
F1F2 = JOIN F1_Filtered BY (dest1, flightDate1),
                   F2_Filtered BY (origin2, flightDate2);

-- Filter F1F2 join where departure of F2 is after the arrival of F1
F1F2_Dept_Arr_Filtered = FILTER F1F2 BY arrTime1 < depTime2;

-- Filter and ensure dates for BOTH F1 & F2 are between 06/2007 and 05/2008
F1F2_FlightDate_Filtered = FILTER F1F2_Dept_Arr_Filtered BY (((year1 == 2007 AND month1 >= 6) OR (year1 == 2008 AND month1 <= 5)) AND ((year2 == 2007 AND month2 >= 6) OR (year2 == 2008 AND month2 <= 5)));

-- Sum the total delay for each F1F2 pair
TotalDelays = FOREACH F1F2_FlightDate_Filtered GENERATE (arrDelayMinutes1 + arrDelayMinutes2) as TotalDelay;

-- Group the flight pairs
GroupedData = GROUP TotalDelays ALL;

-- Generate the count of flights and sum the total delay
AggregatedData = FOREACH GroupedData GENERATE
    COUNT(TotalDelays) AS total_flights,  -- Count of flights
    SUM(TotalDelays.TotalDelay) AS total_delay;  -- Sum of all delays

-- Generate the average delay
Result = FOREACH AggregatedData GENERATE
    total_flights,
    total_delay,
    (double) total_delay / total_flights AS average_delay;

-- Store the output
 STORE Result INTO '$OUTPUT';
