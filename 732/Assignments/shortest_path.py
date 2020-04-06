import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types

def graph_edge_format(line):
    a = line.split(':')
    b = a[1].split()
    i=0
    if len(b) == 0:
        yield a[0],''
    while i < len(b):
        yield a[0],b[i]
        i += 1

def main(inputs, source_node, destination_node):

    schema_spec = types.StructType([
        types.StructField('node', types.StringType(),True),
        types.StructField('source', types.StringType(),True),
        types.StructField('distance', types.StringType(),True)
    ])

    rdd = sc.textFile(inputs)
    graph_edges = rdd.flatMap(graph_edge_format)
    edges_df = spark.createDataFrame(graph_edges, ['source', 'dest']).cache()
    edges_df.createOrReplaceTempView('edges')

    if source_node == destination_node:
        print("The source and node are the same. The shortest path is 0")
        exit()

    if len(graph_edges.lookup(destination_node))==0:
        print("The destination node does not exist")
        exit()

    for i in range(rdd.count()+1):

        if i > 0:
            df_iters = spark.read.csv(f'{output}/iter-{i-1}',schema=schema_spec)
            df_iters.createOrReplaceTempView('iterated_edges')
            drop = df_iters.select('node')
            query2 = f"""SELECT e.dest, ie.node, {i} AS distance
                           FROM iterated_edges ie
                           JOIN edges e
                             ON e.source = ie.node
                            AND e.dest != ''"""
            #Dropping the visited set
            dropper = [int(row['node']) for row in drop.distinct().collect()]
            df_next_iters = spark.sql(query2)
            df_im = df_iters.union(df_next_iters)
            df_im.createOrReplaceTempView('df_im_short')
            edges_df = edges_df.filter(~functions.col("source").isin(dropper))
            edges_df.createOrReplaceTempView('edges')
            df_im1 = df_im.groupBy(df_im['node']).agg(functions.min(df_im.distance).alias('distance_min'))
            df_im1.createOrReplaceTempView('df_im_short1')
            query3 = """SELECT im.node, im.source, im.distance
                          FROM df_im_short im
                          JOIN df_im_short1 im1
                            ON im.node = im1.node
                           AND im.distance = im1.distance_min"""
            df_final = spark.sql(query3)
            df_final.sort('distance').cache()
            df_final.createOrReplaceTempView('df_final_temp')
            df_final.write.csv(output + '/iter-' + str(i),mode='overwrite')
            value = [row['node'] for row in df_final.collect()]
            if str(destination_node) in value:
                break
            i += 1
        query = f"""SELECT {source_node} AS node,
                           '-' AS source,
                           {i} AS distance"""
        df_initial = spark.sql(query).cache()
        df_initial.createOrReplaceTempView('source_mapping')
        df_initial.write.csv(output + '/iter-' + str(i),mode='overwrite')

    #Lookup for shortest path
    df_lookup = df_final.where(df_final.node == destination_node)
    nodes = [row['node'] for row in df_final.collect()]
    if destination_node in nodes:
        shortest_path = [destination_node]
        while(destination_node != source_node):
            val = df_lookup.select(df_lookup.source).collect()[0][0]
            shortest_path.append(val)
            if(val == source_node):
                break
            df_lookup = df_final.where(df_final.node == val)
        shortest_path.reverse()
        print(shortest_path)
        finalpath = sc.parallelize(shortest_path).coalesce(1)
    else:
        finalpath=sc.parallelize(["shortest path not found"]).coalesce(1)
    finalpath.saveAsTextFile(output + '/path')

if __name__ == '__main__':
    conf = SparkConf().setAppName('shortest_path').setMaster('local[1]')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('shortest_path_sql').getOrCreate()
    sc_sql = spark.sparkContext

    assert sc.version >= '2.4'
    assert spark.version >= '2.4'
    sc.setLogLevel('WARN')
    sc_sql.setLogLevel('WARN')

    inputs = sys.argv[1]+'/links-simple-sorted.txt'
    output = sys.argv[2]
    source_node = sys.argv[3]
    destination_node = sys.argv[4]
    main(inputs,source_node,destination_node)
