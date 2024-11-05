Needed columns: cancelled, diverted, arrTime, depTime, ArrDelayMinutes

- all flights between June 2007 - May 2008
- neither flight is cancelled or diverted, check each of these
- F1 has origin ORD -> X
- F2 has desitination X -> JFK
- total delay = delay of F1 + delay of F2
- final all flights matching these and their total delay
- take the average of all these flights for a single average delay


Need to sets of the filtered parent data set

## F1
Filter to keep the following headers:
1. Year
3. Month
6. FlightDate
12. Origin 
13. OriginCityName 
18. Dest 
19. DestCityName 
36. ArrTime
38. ArrDelayMinutes 
42. Cancelled 
44. Diverted 


code will check the following for each row,

if (year && month >= june 2007 and year && month <= May 2008)

if (origin == ORD && dest !== JFK)

if (cancelled == 0 && Diverted == 0)

Then project and return the following F1 table:

F1_Filtered:
1. FlightDate
2. Origin
3. ArrTime
4. ArrDelayMinutes

## F2
Filter to keep the following headers:
1. Year
3. Month
6. FlightDate
12. Origin
13. OriginCityName
18. Dest
19. DestCityName
25. DepTime
36. ArrTime
39. ArrDelayMinutes
42. Cancelled
44. Diverted


code will check the following for each row,

if (year && month >= june 2007 and year && month <= May 2008)

if (origin != ORD && dest == JFK)

if (cancelled == 0 && Diverted == 0)

Then project and return the following F2 table:

F2_Filtered:
1. FlightDate
2. Dest
3. DepTime
4. ArrDelayMinutes

## Join F1 & F2
To join, the following needs to be true for F1.row and F2.row:

- F1.FlightDate == F2.FlightDate
- F1.Origin == ORD && F2.Dest == JFK
- F1.ArrTime < F2.DepTime
- Calculate delay = F1.delay + F2.delay??