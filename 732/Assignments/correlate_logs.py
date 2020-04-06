import sys
import re
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions

def reg_exp(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = line_re.search(line)
    if match is not None:
        a = line_re.split(line)
        b= ' '.join(a).split()
        yield b

def main(inputs):

    rdd = sc.textFile(inputs)
    rdd_filtered = rdd.flatMap(reg_exp)
    rdd_1 = rdd_filtered.map(lambda x: (x[0],x[3]))
    df = spark.createDataFrame(rdd_1,['hostname','bytes'])
    df.createOrReplaceTempView('nasa_weblogs')
    query = """SELECT hostname, 
                      COUNT(1) AS no_of_requests,
                      SUM(bytes) AS sum_request_bytes
                 FROM nasa_weblogs
             GROUP BY hostname"""
    df1 = spark.sql(query)
    df2 = df1.withColumn('squared_requests',functions.pow(df1.no_of_requests,2))
    df3 = df2.withColumn('squared_bytes',functions.pow(df2.sum_request_bytes,2)).drop(df2.hostname)
    df4 = df3.withColumn('request_mul_bytes',(df3.no_of_requests * df3.sum_request_bytes))
    df5 = df4.withColumn('sq_request_mul_bytes', functions.pow(df4.request_mul_bytes,2))
    df5.createOrReplaceTempView('corr_c')
    query1 = """SELECT SUM(no_of_requests) as xi, 
                      SUM(sum_request_bytes) AS yi, 
                      SUM(squared_requests) AS xi2,
                      SUM(squared_bytes) AS yi2,
                      SUM(request_mul_bytes) AS xiyi
                 FROM corr_c"""
    df6 = spark.sql(query1)
    df7 = df6.withColumn('corr_num',((df1.count()*df6.xiyi) - (df6.xi*df6.yi)))
    df8 = df7.withColumn('corr_den', (functions.sqrt((df1.count() * df6.xi2) - functions.pow(df6.xi, 2))*functions.sqrt((df1.count() * df6.yi2) - functions.pow(df6.yi, 2))))
    df_corr = df8.withColumn('correlation',df8.corr_num/df8.corr_den)
    value = df_corr.select("correlation").collect()[0][0]
    print(f'r = {value}')
    print(f'r^2 = {value**2}')


if __name__ == '__main__':
    conf = SparkConf().setAppName('correlate_logs').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('correlate_sqls').getOrCreate()
    sc_sql = spark.sparkContext

    assert sc.version >= '2.4'
    assert spark.version >= '2.4'
    sc.setLogLevel('WARN')
    sc_sql.setLogLevel('WARN')

    inputs = sys.argv[1]
    main(inputs)
