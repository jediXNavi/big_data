import sys
import re

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, functions, types

def graph_edge_format(line):
    a = line.split(':')
    b = a[1].split()
    i=0
    if len(b) == 0:
        yield a[0],''
    while i < len(b):
        yield a[0],b[i]
        i += 1

def drop_visited(dropper,line):
    length_dropper = len(dropper)
    print(length_dropper)
    len_line = len(line)
    print(len_line)
    for i in range(length_dropper):
        for n in range(len_line):
            print("oola")
            print(line[n])
            # if line[n][0] == dropper[i]:
            #     del line[n]
            return line

def main(inputs, source_node, destination_node):

    schema_spec = types.StructType([
        types.StructField('node', types.StringType(),True),
        types.StructField('source', types.StringType(),True),
        types.StructField('distance', types.StringType(),True)
    ])

    rdd = sc.textFile(inputs)
    graph_edges = rdd.flatMap(graph_edge_format)
    edges_df = spark.createDataFrame(graph_edges, ['source', 'dest'])
    edges_df.createOrReplaceTempView('edges')

    for i in range(rdd.count()+1):
        print("The iteration number is" + str(i))

        if i > 0:
            df_iters = spark.read.csv(f'{output}/iter-{i-1}',schema=schema_spec)
            df_iters.createOrReplaceTempView('iterated_edges')
            drop = df_iters.select('node')
            print("Printing g-edge before")
            edges_df.show()
            print("old iteration")
            df_iters.show()
            query2 = f"""SELECT e.dest, ie.node, {i} AS distance
                           FROM iterated_edges ie
                           JOIN edges e
                             ON e.source = ie.node
                            AND e.dest != ''"""
            #Dropping the visited set
            dropper = [int(row['node']) for row in drop.distinct().collect()]
            print(dropper)
            df_next_iters = spark.sql(query2)
            print("that iteration")
            df_next_iters.show()
            df_final = df_iters.union(df_next_iters)
            print("All iterations")
            df_final.show()
            edges_df = edges_df.filter(~functions.col("source").isin(dropper))
            edges_df.createOrReplaceTempView('edges')
            print("Checking g-edge later")
            edges_df.show()
            df_final.write.csv(output + '/iter-' + str(i),mode='overwrite')
            i += 1

        query = f"""SELECT {source_node} AS node,
                           '-' AS source,
                           {i} AS distance"""
        df_initial = spark.sql(query)
        df_initial.createOrReplaceTempView('source_mapping')
        # query1 = f"""SELECT dest, node, ({i}+1) AS distance
        #               FROM edges e
        #         CROSS JOIN source_mapping s
        #                 ON e.source = s.node"""
        # df_1st_iter = spark.sql(query1)
        # df_1st_iter.createOrReplaceTempView('node_map1')
        # df_updated = df_initial.union(df_1st_iter)
        df_initial.write.csv(output + '/iter-' + str(i),mode='overwrite')


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
    output = sys.argv[2]
    source_node = sys.argv[3]
    destination_node = sys.argv[4]
    main(inputs,source_node,destination_node)
