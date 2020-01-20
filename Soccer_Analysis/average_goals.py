import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import types, SparkSession, functions, types
import plotly.graph_objects as go
import matplotlib.pyplot as plt1
import plotly.express as px

def main():
    df = spark.read.format("mongo").options(collection='teams').load()
    df.dropna()
    df = df.withColumn('away_goals', df['away goals'])  # goals
    df = df.withColumn('home_goals', df['home goals']) # goals
    teams = df.select('away name', 'league').distinct().orderBy('league')
    teams = teams.groupBy('league').agg(functions.count(teams['away name']).alias('total_teams'))
    teams.createOrReplaceTempView('tot_teams_per_league')
    # teams = teams.filter(teams.league == league_name)
    teams.show(truncate=False)
    league = df.select('league', 'season', 'away_goals', 'home_goals', 'home name').dropna()
    league.createOrReplaceTempView('league')

    query = """SELECT league, season, sum(away_goals) as sum_away_goals, sum(home_goals) as sum_home_goals
                       FROM league
                      WHERE 1=1
                        AND ((away_goals IS NOT NULL OR away_goals!=' ')
                         OR (home_goals IS NOT NULL OR home_goals!=' ')
                         OR (league IS NOT NULL OR league!=' ')
                         OR (season IS NOT NULL OR season!=' '))
                   GROUP BY league, season
                   ORDER BY season"""
    league = spark.sql(query)
    league.createOrReplaceTempView('league_total_goals')
    league.show()
    query2 = """SELECT league, season,(avg_home_goal_pleague + avg_away_goal_pleague) as avg_goals
                  FROM(
                SELECT l.league,l.season, (sum_away_goals/total_teams) AS avg_home_goal_pleague, (sum_home_goals/total_teams) AS avg_away_goal_pleague
                  FROM league_total_goals l
                  JOIN tot_teams_per_league t
                    ON l.league = t.league)"""
    league_data = spark.sql(query2)
    league_data.show()
    years = league_data.select('season').distinct().collect()
    league_data = league_data.toPandas()
    # have to plot the graph
    leagues = ['Bundesliga','Ligue 1','Primera Division','Premier League','Serie A']


    fig = px.line(league_data, x='season', y='avg_goals', color='league')

    fig.update_layout(
        autosize=False,
        width=1600,
        height=1600,
        margin=go.layout.Margin(
            l=60,
            r=60,
            b=120,
            t=120,
            pad=4))

    fig.update_layout(
        xaxis=go.layout.XAxis(
            tickangle=270,
            tickfont=dict(size=12),
            title_font={"size": 20}),
        yaxis=go.layout.YAxis(tickfont=dict(size=12)))

    fig.update_layout(
        legend=go.layout.Legend(
            x=0,
            y=1,
            traceorder="normal",
            font=dict(
                family="sans-serif",
                size=12,
                color="black"
            ),
            bgcolor="LightSteelBlue",
            bordercolor="Black",
            borderwidth=2
        )
    )

    fig.show()
    # plot_1 = go.Scatter(
    #      x=a, y=d1, name="Average Goals", opacity=0.8)
        # seas = plot_1
    # plot_1.show()
    # plot_2 = go.Scatter(
    #     x=d1, y=c, name="Average Goals", opacity=0.8)
    # plot_3 = go.Scatter(
    #     x=a, y=d, name="Away goals Team2", opacity=0.8)
    # plot_4 = go.Scatter(
    #     x=a, y=e, name="Home goals Team2", opacity=0.8)
    # plot_5 = go.Scatter(
    #     x=a, y=e, name="Home goals Team2", opacity=0.8)
    # plot_data = [plot_1, plot_2, plot_3, plot_4]
    # layout3 = dict(
    #     title="Average goals per season per league"
    # )
    # plot_1.show()


    # d1 = []
    # plt1.title('Average goals per season per league')
    # plt1.xticks(z, a, rotation='vertical')
    # plt1.xlabel('Season')
    # plt1.ylabel('Average Goals')
    # plt1.legend(leagues)
    # plt1.show()

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Visitors') \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/cmpt_732") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/cmpt_732") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    # league_name = sys.argv[1]
    main()