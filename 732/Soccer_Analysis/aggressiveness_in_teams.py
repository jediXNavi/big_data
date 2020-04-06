from pyspark.sql import SparkSession, functions, types
from pyspark import SparkContext, SparkConf
import numpy as np
import plotly.graph_objects as go
import plotly.figure_factory as ff

def aggressiveness_in_teams():

    df = spark.read.format("mongo").options(collection="fouls").load()

    df1 = df.groupBy(df['league']).agg(functions.count('Team').alias('team_count'))
    df3 = df.groupBy(df['league']).agg(functions.sum(df['Goals']).alias('Total_Goals'))
    df4 = df.groupBy(df['league']).agg(functions.sum(df['Possession%']).alias('Total_Possession'))
    df5 = df.groupBy(df['league']).agg(functions.sum(df['Red Cards']).alias('Total_RedCards'))
    df6 = df.groupBy(df['league']).agg(functions.sum(df['Shots pg']).alias('Total_SPG'))
    df7 = df.groupBy(df['league']).agg(functions.sum(df['Yellow Cards']).alias('Total_YellowCards'))
    df8 = df.groupBy(df['league']).agg(functions.sum(df['AerialsWon']).alias('Total_AerialsWon'))
    df8.show()
    df_join = df3.join(df1,df1.league == df3.league).drop(df1['league'])
    df_join = df_join.join(df4,df4.league == df_join.league).drop(df4['league'])
    df_join = df_join.join(df5,df5.league == df_join.league).drop(df5['league'])
    df_join = df_join.join(df6,df6.league == df_join.league).drop(df6['league'])
    df_join = df_join.join(df7,df7.league == df_join.league).drop(df7['league'])
    df_join = df_join.join(df8,df8.league == df_join.league).drop(df7['league'])

    df9 = df_join.withColumn('average_Goals', df3.Total_Goals / df1.team_count) \
        .withColumn('average_Possession', df4.Total_Possession / df1.team_count) \
        .withColumn('average_RedCards', df5.Total_RedCards / df1.team_count) \
        .withColumn('average_SPG', df6.Total_SPG / df1.team_count) \
        .withColumn('average_YellowCards', df7.Total_YellowCards / df1.team_count) \
        .withColumn('average_aerials_won',df8.Total_AerialsWon/ df1.team_count) \
        .drop('Total_OutOfBox', 'Total_Penalty', 'Total_SixYardBox','Total_AerialsWon')
    df9.show()

    aerials_won = [row['average_aerials_won'] for row in df9.collect()]
    avg_goals = [row['average_Goals'] for row in df9.collect()]
    pos = [row['average_Possession'] for row in df9.collect()]
    redcards = [row['average_RedCards'] for row in df9.collect()]
    shots_pg = [row['average_SPG'] for row in df9.collect()]
    yellow_cards = [row['average_YellowCards'] for row in df9.collect()]
    leagues = [row['league'] for row in df9.collect()]

    fig = go.Figure(data=[
            go.Bar(name='average_Goals', x=leagues, y=avg_goals),
            go.Bar(name='average_Possession', x=leagues, y=pos),
            go.Bar(name='average_RedCards', x=leagues, y=redcards),
            go.Bar(name='average_SPG', x=leagues, y=shots_pg),
            go.Bar(name='average_YellowCards', x=leagues, y=yellow_cards)
        ])
    # Change the bar mode
    fig.update_layout(title='Aggressive Outlook in Leagues',barmode='group')
    fig.show()
    df = df9.select('average_aerials_won','average_Goals', 'average_Possession', 'average_SPG', 'average_YellowCards', 'average_RedCards')
    df = df.toPandas()
    labels = ['AerialsWon', 'Goals', 'Possession', 'Shots pg', 'Yellow Cards', 'Red Cards']

    corr = df.corr().to_numpy().round(2)
    fig = ff.create_annotated_heatmap(z=corr,
                                      annotation_text=corr,
                                      x=labels,
                                      y=labels,
                                      hoverongaps=False,
                                      colorscale='brwnyl')
    fig.show()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Football_Analysis') \
            .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/cmpt_732") \
            .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    aggressiveness_in_teams()