import sys

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('Temp_range').getOrCreate()
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

    df= spark.read.csv(inputs,schema=weather_schema)
    df1= df.filter(df.qflag.isNull()).select('station','date','observation','value').cache()
    df_tmax = df1.where(df1['observation']=='TMAX').withColumnRenamed('value','t_max')
    df_tmin = df1.where(df1['observation']=='TMIN').withColumnRenamed('value','t_min')
    df_range = df_tmax.join(df_tmin,((df_tmax.date==df_tmin.date) & (df_tmin.station == df_tmax.station)),'inner').select(df_tmax['station'],df_tmax['date'],df_tmax['t_max'],df_tmin['t_min']).withColumn('diff',((df_tmax['t_max']-df_tmin['t_min'])/10))
    #Caching as we will be joining to find the station having max temp difference
    df_max_range = df_range.groupBy(df_range['date']).agg(functions.max(df_range['diff']).alias('range')).cache()
    df_output = df_max_range.join(df_range,((df_max_range.date == df_range.date) & (df_max_range.range == df_range.diff)),'inner').select(df_max_range['date'],'station','range').sort('date','station')
    df_output.write.csv(output,compression='gzip',mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)