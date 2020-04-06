import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions

def main(ip_keyspace, op_keyspace):
    query1 = f"""CREATE OR REPLACE TEMPORARY VIEW orders
                        USING org.apache.spark.sql.cassandra
                      OPTIONS(
                      table "orders",
                      keyspace "{ip_keyspace}")"""
    spark.sql(query1)
    query2 = f"""CREATE OR REPLACE TEMPORARY VIEW lineitem
                        USING org.apache.spark.sql.cassandra
                      OPTIONS(
                      table "lineitem",
                      keyspace "{ip_keyspace}")"""
    spark.sql(query2)
    query3 = f"""CREATE OR REPLACE TEMPORARY VIEW part
                        USING org.apache.spark.sql.cassandra
                      OPTIONS(
                      table "part",
                      keyspace "{ip_keyspace}")"""
    spark.sql(query3)
    query4 = f'''SELECT o.orderkey,o.totalprice, collect_set(p.name) AS part_names
                  FROM orders o
                  JOIN lineitem l
                    ON o.orderkey = l.orderkey
                  JOIN part p
                    ON l.partkey = p.partkey
                 WHERE 1=1
              GROUP BY o.orderkey, o.totalprice
              ORDER BY o.orderkey'''
    df_im = spark.sql(query4)
    df_im.createOrReplaceTempView('part_data')
    query = f'''SELECT o.*, p.part_names 
                  FROM orders o
                  JOIN part_data p
                    ON o.orderkey = p.orderkey'''
    final_df = spark.sql(query)
    final_df.write.format("org.apache.spark.sql.cassandra").options(table='orders_parts',keyspace = op_keyspace).save()

if __name__ == '__main__':
    cluster_seeds = ['199.60.17.32','199.60.17.65']
    conf = SparkConf().setAppName('tpch_spark_rdd')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('tpch_orders_spark_sql').config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
    sc_sql = spark.sparkContext
    sc_sql.setLogLevel('WARN')

    ip_keyspace = sys.argv[1]
    op_keyspace = sys.argv[2]
    main(ip_keyspace,op_keyspace)
