/*
This is HiveQL script which processes sample US flight delays data and queries
the data to analyse the various delay happens in airlines per year from 2003 to 2010.

Need to set following hivevar variable as below to set which delay type is to process.
--hivevar delay_type_col_name=CarrierDelay
--hiveconf hive.session.id=calculate-flight-delay-CarrierDelay-1
--hiveconf hive.execution.engine=mr
*/

DROP TABLE IF EXISTS delay_flights;

CREATE EXTERNAL TABLE delay_flights
(
    ID                int,
    Year              int,
    Month             int,
    DayofMonth        int,
    DayOfWeek         int,
    DepTime           int,
    CRSDepTime        int,
    ArrTime           int,
    CRSArrTime        int,
    UniqueCarrier     string,
    FlightNum         int,
    TailNum           int,
    ActualElapsedTime int,
    CRSElapsedTime    int,
    AirTime           int,
    ArrDelay          int,
    DepDelay          int,
    Origin            string,
    Dest              string,
    Distance          int,
    TaxiIn            int,
    TaxiOut           int,
    Cancelled         int,
    CancellationCode  string,
    Diverted          boolean,
    CarrierDelay      int,
    WeatherDelay      int,
    NASDelay          int,
    SecurityDelay     int,
    LateAircraftDelay int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION "${INPUT}";

SET hive.execution.engine=mr;

INSERT OVERWRITE DIRECTORY "${OUTPUT}/${delay_type_col_name}"
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT Year, avg((${delay_type_col_name} / ArrDelay) * 100)
FROM delay_flights
GROUP BY Year;

