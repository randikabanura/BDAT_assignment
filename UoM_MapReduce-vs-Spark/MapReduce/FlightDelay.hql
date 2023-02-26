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

INSERT OVERWRITE DIRECTORY "${OUTPUT}"
SELECT Year, avg((CarrierDelay / ArrDelay) * 100)
                                                      FROM delay_flights
                                                      GROUP BY Year;

