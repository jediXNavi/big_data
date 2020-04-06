import sys
import re

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('wiki_popular').getOrCreate()
assert spark.version >= '2.4'
sc = spark.sparkContext
sc.setLogLevel('WARN')

@functions.udf(returnType=types.StringType())
def splitter(line):
    a = line.rsplit("/",1)
    b = a[1].strip("pagecounts-.gz")
    return b[:-2]


def isBoring(line):
    return not (line.startswith(("Main_Page", "Special:")))

def main(inputs, output):

    wiki_schema = types.StructType([
        types.StructField('language', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('views',types.IntegerType()),
        types.StructField('bytes',types.LongType())])

    df = spark.read.csv(inputs,sep=' ',schema=wiki_schema).withColumn('filename', functions.input_file_name())
    df2 = df.withColumn('hour',splitter(df['filename'])).drop(df['filename'])
    df3 = df2.filter((functions.col("language")=='en') & (functions.col("title") != "Main_Page"))
    df4 = df3.where(~df3.title.startswith("Special:")).cache()
    df5 = df4.groupBy(df4['hour']).agg(functions.max(df4['views']).alias('max_views'))
    df6= functions.broadcast(df5).join(df4, ((df4.views == df5.max_views) & (df4.hour == df5.hour)),'inner').select(df5.hour, df4.title, df5.max_views).orderBy('hour','title')
    df6.write.json(output,compression=None,mode='overwrite')
    df6.show(20, False)
    df6.explain()

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)