import sys

from pyspark.sql import SparkSession

def output_line(orderkey, price, names):
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)

def main(keyspace,outdir,orderkeys):
    orderkey = tuple(map(int, orderkeys))
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=keyspace).load()
    df.createOrReplaceTempView('temp')
    query = f'''SELECT t.orderkey, t.totalprice, t.part_names
                 FROM temp t
                WHERE 1=1
                  AND orderkey IN {orderkey}
             ORDER BY t.orderkey'''
    temp_df = spark.sql(query)
    temp_rdd = temp_df.rdd
    final_rdd = temp_rdd.map(lambda x: output_line(x[0],x[1],x[2]))
    final_rdd.saveAsTextFile(outdir)


if __name__ == '__main__':
    cluster_seeds = ['199.60.17.32','199.60.17.65']
    spark = SparkSession.builder.appName('tpch_ord_denorm').config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
    sc_sql = spark.sparkContext
    sc_sql.setLogLevel('WARN')
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace,outdir,orderkeys)
