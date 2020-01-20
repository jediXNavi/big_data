import sys

from pyspark.sql import SparkSession, functions, types


def main(keyspace, table_name):
    df = spark.read.format('org.apache.spark.sql.cassandra').options(table = table_name, keyspace = keyspace).load()
    df.createOrReplaceTempView('nasa_weblogs')
    query = """SELECT host, 
                      COUNT(1) AS no_of_requests,
                      SUM(bytes) AS sum_request_bytes
                 FROM nasa_weblogs
             GROUP BY host"""
    df1 = spark.sql(query)
    df2 = df1.withColumn('squared_requests',functions.pow(df1.no_of_requests,2))
    df3 = df2.withColumn('squared_bytes',functions.pow(df2.sum_request_bytes,2)).drop(df2.host)
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
    cluster_seeds = ['199.60.17.32','199.60.17.65']
    spark = SparkSession.builder.appName('Cassandra_corr').config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
    keyspace = sys.argv[1]
    table_name = sys.argv[2]
    main(keyspace, table_name)
