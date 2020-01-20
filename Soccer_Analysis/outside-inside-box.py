from pyspark.sql import SparkSession, functions
import plotly.graph_objects as go



def outside_inside_box():
    df = spark.read.format("mongo").options(collection="shot_placement").load()
    df1 = df.groupBy(df['Tournament']).agg(functions.count('Team').alias('team_count'))
    df2 = df.groupBy(df['Tournament']).agg(functions.sum(df['OutOfBox']).alias('Total_OutOfBox'))
    df3 = df.groupBy(df['Tournament']).agg(functions.sum(df['PenaltyArea']).alias('Total_Penalty'))
    df4 = df.groupBy(df['Tournament']).agg(functions.sum(df['SixYardBox']).alias('Total_SixYardBox'))
    df5 = df2.join(df1,df1.Tournament == df2.Tournament).drop(df1['Tournament'])
    df6 = df5.join(df3,df3.Tournament == df5.Tournament).drop(df3['Tournament'])
    df7 = df6.join(df4,df4.Tournament == df6.Tournament).drop(df4['Tournament'])
    df8 = df7.withColumn('average_OutOfBox', df2.Total_OutOfBox/df1.team_count)\
        .withColumn('average_PenaltyArea',df3.Total_Penalty/df1.team_count)\
        .withColumn('average_SixYardBox', df4.Total_SixYardBox/df1.team_count)\
        .drop('Total_OutOfBox', 'Total_Penalty', 'Total_SixYardBox')

    oob = [row['average_OutOfBox'] for row in df8.collect()]
    pa = [row['average_PenaltyArea'] for row in df8.collect()]
    syb = [row['average_SixYardBox'] for row in df8.collect()]
    leagues = [row['Tournament'] for row in df8.collect()]

    fig = go.Figure(data=[
            go.Bar(name='Average goals from outisde the box', x=leagues, y=oob),
            go.Bar(name='Average goals from the penalty area', x=leagues, y=pa),
            go.Bar(name='Average goals from the six yard box', x=leagues, y=syb)
    ])
    fig.update_layout(title='Shot Placement in Different Leagues',barmode='group')
    fig.show()

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Football_Analysis') \
            .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/cmpt_732") \
            .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    outside_inside_box()
