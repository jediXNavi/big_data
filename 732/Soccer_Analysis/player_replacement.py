from pyspark.sql import SparkSession, functions
import sys
import plotly.graph_objects as godef player_replacement(input_player):
    players_data = spark.read.format("mongo").options(collection="players2").load()
    players_e = players_data.select('short_name', 'age', 'club', 'overall', 'potential', 'value_eur', 'wage_eur', 'team_position', 'work_rate', 'pace', 'shooting',
                                    'passing', 'dribbling', 'defending')    players_e2 = players_e.where(players_e['short_name'] == input_player)
    input_age = players_e2.select(players_e2['age']).collect()
    # print(input_age[0][0])
    input_potential = players_e2.select(players_e2['potential']).collect()
    # print(input_potential[0][0])
    input_pos = players_e2.select(players_e2['team_position']).collect()
    # print(input_pos[0][0])    players_e = players_e.where(players_e['team_position'] == input_pos[0][0]).orderBy('age')
    players_e = players_e.where(players_e['age'] < input_age[0][0]).where(players_e['potential'] >= input_potential[0][0])
    players_e.show()if __name__ == '__main__':
    spark = SparkSession.builder.appName('Football_Analysis') \
            .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/cmpt_732") \
            .getOrCreate()    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    player = sys.argv[1]
    player_replacement(player)