import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions

def output_line(orderkey, price, names):
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)

def main(keyspace, outdir, orderkeys):
    orderkey = tuple(map(int, orderkeys))
    query1 = f"""CREATE OR REPLACE TEMPORARY VIEW orders
                        USING org.apache.spark.sql.cassandra
                      OPTIONS(
                      table "orders",
                      keyspace "{keyspace}")"""
    spark.sql(query1)
    query2 = f"""CREATE OR REPLACE TEMPORARY VIEW lineitem
                        USING org.apache.spark.sql.cassandra
                      OPTIONS(
                      table "lineitem",
                      keyspace "{keyspace}")"""
    spark.sql(query2)
    query3 = f"""CREATE OR REPLACE TEMPORARY VIEW part
                        USING org.apache.spark.sql.cassandra
                      OPTIONS(
                      table "part",
                      keyspace "{keyspace}")"""
    spark.sql(query3)
    query = f'''SELECT o.orderkey,o.totalprice, collect_set(p.name)
                  FROM orders o
                  JOIN lineitem l
                    ON o.orderkey = l.orderkey
                  JOIN part p
                    ON l.partkey = p.partkey
                 WHERE 1=1
                   AND o.orderkey IN {orderkey}
              GROUP BY o.orderkey, o.totalprice
              ORDER BY o.orderkey'''
    final_df = spark.sql(query)
    final_df.explain()
    rdd_data = final_df.rdd
    output_rdd = rdd_data.map(lambda x: output_line(x[0],x[1],x[2]))
    output_rdd.saveAsTextFile(outdir)

if __name__ == '__main__':
    cluster_seeds = ['199.60.17.32','199.60.17.65']
    conf = SparkConf().setAppName('tpch_spark_rdd')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('tpch_orders_spark_sql').config('spark.cassandra.connection.host',','.join(cluster_seeds))\
            .config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
    sc_sql = spark.sparkContext
    sc_sql.setLogLevel('WARN')

    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace,outdir,orderkeys)
