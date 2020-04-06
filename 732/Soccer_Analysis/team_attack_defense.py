import sys
from pyspark.sql import SparkSession, functions
import plotly.express as px

def main(team_a, team_b):
    players_data = spark.read.format("mongo").options(collection='players').load()

    players_g1 = players_data.select('A/W', 'Team& Contract').groupBy('Team& Contract', 'A/W').agg(
        functions.count('A/W').alias('Total Attacking Work Rate'))
    players_g1 = players_g1.where(players_g1['A/W'] != 'N/A')
    df_one_a = players_g1.where(players_g1['Team& Contract'] == team_a)
    df_one_b = players_g1.where(players_g1['Team& Contract'] == team_b)

    players_g2 = players_data.select('D/W','Team& Contract').groupBy('Team& Contract','D/W').agg(functions.count('D/W').alias('Total Defensive Work Rate'))
    players_g2 = players_g2.where(players_g2['D/W']!='N/A')
    df_two_a = players_g2.where(players_g2['Team& Contract']==team_a)
    df_two_b = players_g2.where(players_g2['Team& Contract']==team_b)

    df_one_a = df_one_a.unionAll(df_one_b).distinct().orderBy('Team& Contract','A/W')
    df_two_a = df_two_a.unionAll(df_two_b).distinct().orderBy('Team& Contract','D/W')
    df_one_a = df_one_a.withColumnRenamed('Team& Contract', 'Team Name').withColumnRenamed('A/W', 'Attacking Work Rate')
    df_two_a = df_two_a.withColumnRenamed('Team& Contract', 'Team Name').withColumnRenamed('D/W', 'Defensive Work Rate')
    df_one_a = df_one_a.toPandas()
    df_two_a= df_two_a.toPandas()

#-------------------------------plotting the graph-----------------------------------------#
    fig = px.bar(df_one_a, x="Attacking Work Rate", y="Total Attacking Work Rate", color="Team Name", barmode="group", height=400)
    fig.show()
    fig1 = px.bar(df_two_a, x="Defensive Work Rate", y="Total Defensive Work Rate", color="Team Name", barmode="group", height=400)
    fig1.show()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Team Attack and defense matrix') \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/cmpt_732") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/cmpt_732") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    team1 = sys.argv[1]
    team2 = sys.argv[2]
    main(team1, team2)
