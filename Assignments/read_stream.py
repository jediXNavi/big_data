import sys
from pyspark.sql import SparkSession, functions, types

def main(topic):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
        .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string').alias('val'))
    splitter = functions.split(values['val'],' ')
    values_df = values.withColumn('x',splitter.getItem(0)).withColumn('y',splitter.getItem(1)).withColumn('counter',functions.lit(1)).drop('val')
    values_df.createOrReplaceTempView('xy_data')
    query = '''SELECT SUM(x) as x_sum,
                      SUM(y) as y_sum,
                      SUM(x*y) AS xy_sum,
                      SUM(pow(x,2)) AS x2_sum,
                      pow(SUM(x),2) AS x_sum2,
                      SUM(counter) AS n
                 FROM xy_data'''
    df_beta_temp = spark.sql(query)
    df_beta_num = df_beta_temp.withColumn('beta_num',((functions.col("n")*functions.col("xy_sum") - (functions.col("x_sum")*functions.col("y_sum")))/functions.col("n")))
    df_beta_den = df_beta_num.withColumn('beta_den',((functions.col("n")*functions.col("x2_sum")- functions.col("x_sum2"))/functions.col("n")))
    df_beta = df_beta_den.withColumn('beta',functions.col("beta_num")/functions.col("beta_den"))
    df_alpha = df_beta.withColumn('alpha',((functions.col("y_sum") - (functions.col("beta")*functions.col("x_sum")))/functions.col("n")))
    final_df = df_alpha.select('beta','alpha')
    result = final_df.writeStream.outputMode('complete').format('console').options(truncate='False').start()
    result.awaitTermination(600)

if __name__ == '__main__':
    spark = SparkSession.builder.appName('kafka-spark').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    topic = sys.argv[1]
    main(topic)
