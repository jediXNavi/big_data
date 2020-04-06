import sys

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('temp_range_Sql').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('WARN')

def main(inputs, output):

    weather_schema = types.StructType([
        types.StructField('station',types.StringType()),
        types.StructField('date',types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType())
    ])
    df = spark.read.csv(inputs,schema=weather_schema)
    df.createOrReplaceTempView("weather_data")
    query1 = """SELECT station,
                      date, 
                      observation,
                      value
                 FROM weather_data w
                WHERE 1=1
                  AND w.qflag IS NULL
                  AND w.observation = 'TMAX'"""
    df1 = spark.sql(query1)
    df1.createOrReplaceTempView("max_temp_data")
    query2 = """SELECT station,
                      date, 
                      observation,
                      value
                 FROM weather_data w
                WHERE 1=1
                  AND w.qflag IS NULL
                  AND w.observation = 'TMIN'"""
    df2 = spark.sql(query2)
    df2.createOrReplaceTempView("min_temp_data")

    query3= """SELECT date, station, (t_max - t_min) AS diff
                 FROM ( SELECT mxt.station, 
                               mxt.date, 
                               (mxt.value/10) t_max, 
                               (mit.value/10) t_min
                          FROM max_temp_data mxt
                          JOIN min_temp_data mit
                            ON mxt.station = mit.station
                           AND mxt.date = mit.date)"""

    df3 = spark.sql(query3)
    df3.createOrReplaceTempView("temp_diff")

    query4= """SELECT date, 
                      MAX(diff) AS range
                 FROM temp_diff
             GROUP BY date"""
    df4= spark.sql(query4)
    df4.createOrReplaceTempView("max_range")

    query5="""SELECT /*+ BROADCAST(m) */ t.date, t.station, round(range,2) AS range
                FROM temp_diff t
                JOIN max_range m
                  ON t.date = m.date
                 AND t.diff = m.range
            ORDER BY date, station"""
    df5 = spark.sql(query5)
    df5.explain()

    df5.write.csv(output,compression='gzip',mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)

    """/*+ BROADCAST(m) */"""