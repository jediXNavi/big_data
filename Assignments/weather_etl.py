import sys
assert sys.version_info >= (3,5)

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather_etl').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main(inputs,output):

    observation_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])

    df= spark.read.csv(inputs, schema=observation_schema)
    df1 = df.filter(df.qflag.isNull())
    df2 = df1.filter(df1.station.startswith('CA'))
    df3 = df2.where(df2['observation']=='TMAX')
    df4= df3.withColumn('tmax', df3.value/10)
    final_df = df4.select("station","date","tmax")

    final_df.write.json(output, compression='gzip', mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
