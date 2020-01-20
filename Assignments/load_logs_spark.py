import sys
import re
import uuid
import datetime as d
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions, types

def reg_exp(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = line_re.search(line)
    if match is not None:
        a = line_re.split(line)
        b= ' '.join(a).split()
        yield b

def main(inputs,keyspace,table):
    #Generating the UUID
    uuidUdf= functions.udf(lambda : str(uuid.uuid4()))

    rdd = sc.textFile(inputs)
    rdd_filtered = rdd.flatMap(reg_exp)
    rdd_1 = rdd_filtered.map(lambda x: (x[0],d.datetime.strptime(x[1],"%d/%b/%Y:%H:%M:%S"),x[2],int(x[3])))
    df = spark.createDataFrame(rdd_1,['host','datetime','path','bytes']).repartition(18)
    df1= df.withColumn('uid',uuidUdf()).select('host','uid','bytes','datetime','path')
    df1.write.format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace).save()


if __name__ == '__main__':
    conf = SparkConf().setAppName('spark cassandra example')
    sc = SparkContext(conf=conf)
    cluster_seeds = ['199.60.17.32', '199.60.17.65']
    spark = SparkSession.builder.appName('spark cassandra example'). \
        config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    sc_sql = spark.sparkContext

    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(inputs, keyspace, table)

