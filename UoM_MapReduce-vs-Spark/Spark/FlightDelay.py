import argparse
from pyspark.sql import SparkSession


def calculate_flight_delays(data_source, output_uri, delay_type_col_name='CarrierDelay', iterations=1):
    """
    Processes sample US flight delays data and queries the data to analyse the various dela
    y happens in airlines per year  from 2003 to 2010.

    :param data_source: The URI of US flight delays data CSV, such as 's3://DOC-EXAMPLE-BUCKET/DelayedFlights-updated.csv'.
    :param output_uri: The URI where output is written, such as 's3://DOC-EXAMPLE-BUCKET/flight_delays_results'.
    :param delay_type_col_name: Colum name of delayed flight type (carrier delay, NAS delay, etc.).
    :param iterations: Number of times query need to execute.
    """
    with SparkSession.builder.appName(
            f"Calculate Flight Delay - {delay_type_col_name} - {iterations}").getOrCreate() as spark:
        # Load the restaurant violation CSV data
        if data_source is not None:
            flights_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        flights_df.createOrReplaceTempView("delay_flights")

        # Create a DataFrame of the flight delay averages
        if iterations is None or iterations < 1:
            iterations = 1

        if iterations == 1:
            flight_delay_averages = spark.sql(f"""SELECT Year, avg(({delay_type_col_name} / ArrDelay) * 100) 
                                                  FROM delay_flights 
                                                  GROUP BY Year""")

            # Write the results to the specified output URI
            flight_delay_averages.repartition(1).write.option("header", "true").mode("overwrite").csv(
                "{}/{}".format(output_uri, delay_type_col_name))
        else:
            for index in range(iterations):
                flight_delay_averages = spark.sql(f"""SELECT Year, avg(({delay_type_col_name} / ArrDelay) * 100) 
                                                      FROM delay_flights 
                                                      GROUP BY Year""")

                # Write the results to the specified output URI
                flight_delay_averages.repartition(1).write.option("header", "true").mode("overwrite").csv(
                    "{}/{}/{}/{}".format(output_uri, 'iterations', delay_type_col_name, index + 1))

"""
This function can be called with the arguments such as following:
--data_source s3://bdat-assignment/input/DelayedFlights-updated.csv
--output_uri s3://bdat-assignment/output --delay_type_col_name CarrierDelay
"""
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for you CSV restaurant data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    parser.add_argument(
        '--delay_type_col_name', help="Colum name of delayed flight type (carrier delay, NAS delay, etc.).")
    parser.add_argument(
        '--iterations', type=int, help="Number of times query need to execute.")
    args = parser.parse_args()

    if args.iterations is None:
        args.iterations = 1

    if args.delay_type_col_name is None:
        args.delay_type_col_name = 'CarrierDelay'

    calculate_flight_delays(args.data_source, args.output_uri, args.delay_type_col_name, args.iterations)
