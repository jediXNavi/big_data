import sys
from pyspark.sql import SparkSession, functions, types
import plotly.graph_objects as go



def main(team1, team2):
    df = spark.read.format("mongo").options(collection='teams').load()
    match_results = df.select(df['away goals'], df['away name'], df['season'], df['gameday'], df['home name'],
                                    df['home goals'])

    sub_results1 = match_results.where(df['home name'] == team1)
    sub_results2 = match_results.where(df['away name'] == team1)
    sub_results3 = match_results.where(df['home name'] == team2)
    sub_results4 = match_results.where(df['away name'] == team2)

    results = match_results.withColumn('results', \
                                       functions.when((functions.col('away goals') > functions.col('home goals')),
                                                      'away') \
                                       .when((functions.col('away goals') < functions.col('home goals')), 'home') \
                                       .otherwise('draw'))

    wins_home = results.where(results['home name'] == team1).where(results.results == 'home')
    wins_away = results.where(results['away name'] == team1).where(results.results == 'away')
    loss_home = results.where(results['home name'] == team1).where(results.results == 'away')
    loss_away = results.where(results['away name'] == team1).where(results.results == 'home')
    draws_home = results.where(results['home name'] == team1).where(results.results == 'draw')
    draws_away = results.where(results['away name'] == team1).where(results.results == 'draw')

    labels = 'home wins', 'home lose', 'home draws', 'away wins', 'away lose', 'away draws'
    sizes = [wins_home.count(), loss_home.count(), draws_home.count(), wins_away.count(), loss_away.count(),
             draws_away.count()]

    fig = go.Figure(data = [go.Pie(values=sizes, labels=labels)])
    fig.update_layout(
        title_text=f"Home Advantage For {team1} ")

    wins_home = results.where(results['home name'] == team2).where(results.results == 'home')
    wins_away = results.where(results['away name'] == team2).where(results.results == 'away')
    loss_home = results.where(results['home name'] == team2).where(results.results == 'away')
    loss_away = results.where(results['away name'] == team2).where(results.results == 'home')
    draws_home = results.where(results['home name'] == team2).where(results.results == 'draw')
    draws_away = results.where(results['away name'] == team2).where(results.results == 'draw')

    labels2 = 'home wins', 'home lose', 'home draws', 'away wins', 'away lose', 'away draws'
    sizes2 = [wins_home.count(), loss_home.count(), draws_home.count(), wins_away.count(), loss_away.count(),
             draws_away.count()]

    fig2 = go.Figure(data=[go.Pie(values=sizes2, labels=labels2)])
    fig2.update_layout(
        title_text=f"Home Advantage For {team2} ")

    sub_results1 = sub_results1.select(sub_results1['away goals'], sub_results1['away name'], sub_results1['season'],
                                       sub_results1['gameday'], sub_results1['home name'], sub_results1['home goals'])
    sub_results2 = sub_results2.select(sub_results2['away goals'], sub_results2['away name'], sub_results2['season'],
                                       sub_results2['gameday'], sub_results2['home name'], sub_results2['home goals'])
    sub_results3 = sub_results3.select(sub_results3['away goals'], sub_results3['away name'], sub_results3['season'],
                                       sub_results3['gameday'], sub_results3['home name'], sub_results3['home goals'])
    sub_results4 = sub_results4.select(sub_results4['away goals'], sub_results4['away name'], sub_results4['season'],
                                       sub_results4['gameday'], sub_results4['home name'], sub_results4['home goals'])

    fcb_home = sub_results1.groupby(sub_results1['season']).agg(
        functions.sum(sub_results1['home goals']).alias('_c36')).orderBy('season')
    fcb_away = sub_results2.groupby(sub_results2['season']).agg(
        functions.sum(sub_results2['home goals']).alias('_c36')).orderBy('season')
    rm_home = sub_results3.groupby(sub_results3['season']).agg(
        functions.sum(sub_results3['home goals']).alias('_c36')).orderBy('season')
    rm_away = sub_results4.groupby(sub_results4['season']).agg(
        functions.sum(sub_results4['home goals']).alias('_c36')).orderBy('season')

    a = []
    b = []
    c = []
    d = []
    e = []
    x1 = fcb_away.select(fcb_away['season']).collect()
    x2 = fcb_away.select(fcb_away._c36).collect()
    x3 = fcb_home.select(fcb_home._c36).collect()
    x4 = rm_away.select(rm_away._c36).collect()
    x5 = rm_home.select(rm_home._c36).collect()
    for i in range(0, fcb_away.count()):
        a.append(str(x1[i][0]))
        b.append(int(x2[i][0]))
        c.append(int(x3[i][0]))
        d.append(int(x4[i][0]))
        e.append(int(x5[i][0]))

    plot_1 = go.Scatter(
        x=a,y=b, name=f"Away goals {team1}", opacity=0.8)
    plot_2 = go.Scatter(
        x=a, y=c, name=f"Home goals {team1}",opacity=0.8)
    plot_3 = go.Scatter(
        x=a, y=d, name=f"Away goals {team2}", opacity=0.8)
    plot_4 = go.Scatter(
        x=a, y=e, name=f"Home goals {team2}", opacity=0.8)
    plot_data = [plot_1,plot_2,plot_3,plot_4]
    layout3 = dict(
        title="Home goals v/s Away goals")
    fig3 = go.Figure(data=plot_data, layout=layout3)
    fig.show()
    fig2.show()
    fig3.show()


    players_data = spark.read.format("mongo").options(collection='players2').load()
    players_data = players_data.withColumn('age', players_data['age'].cast(types.IntegerType()))
    players_data = players_data.filter(players_data['age'].isNotNull())
    players_data_mean = players_data.agg(functions.mean('age').alias('mean_age')).collect()
    mean_age = players_data_mean[0][0]

    players_data = players_data.withColumn('OorY', \
                                           functions.when((functions.col('age') <= mean_age), 'Y') \
                                           .otherwise('O'))
    players_data = players_data.select('club','short_name', 'age', 'OorY', 'body_type')

    players_agg = players_data.groupBy('club').agg(functions.count('OorY').alias('count_of_each_club'))


    sr_1 = sub_results1.select('home name')
    pd_1 = players_data.select('OorY', 'club')
    match_players = pd_1.join(sr_1, pd_1['club'] == sr_1['home name']).drop('home name')

    sr_2 = sub_results3.select('home name')
    match_players2 = pd_1.join(sr_2, pd_1['club'] == sr_2['home name']).drop('home name')

    match_players_agg = match_players.groupBy('OorY').agg(functions.count('OorY')).collect()
    match_players_agg2 = match_players2.groupBy('OorY').agg(functions.count('OorY')).collect()
    young1 = match_players_agg[0][1]
    old1 = match_players_agg[1][1]
    young2 = match_players_agg2[0][1]
    old2 = match_players_agg2[1][1]

    labels4 = ['Young ' + str(young1 / (young1 + old1) * 100) + '%', 'Old ' + str(old1 / (young1 + old1) * 100) + '%']
    size_of_groups = [young1, old1]

    fig4 = go.Figure(data=[go.Pie(labels=labels4,values=size_of_groups, hole=.5)])
    fig4.update_layout(
        title_text=f"Player Composition of {team1}")
    fig4.show()

    labels = ['Young ' + str(young2 / (young2 + old2) * 100) + '%', 'Old ' + str(old2 / (young2 + old2) * 100) + '%']
    size_of_groups = [young2, old2]

    fig4 = go.Figure(data=[go.Pie(labels=labels, values=size_of_groups, hole=.5)])
    fig4.update_layout(
        title_text=f"Player Composition of {team2}")
    fig4.show()


    fcb_rm = results.where(((results['away name'] == team1) & (results['home name'] == team2)) | (
                (results['away name'] == team2) & (results['home name'] == team1)))
    fcb_won = fcb_rm.where(((fcb_rm['away name'] == team1) & (fcb_rm.results == 'away')) | (
                (fcb_rm['home name'] == team1) & (fcb_rm.results == 'home')))
    rm_won = fcb_rm.where((fcb_rm['away name'] == team2) & (fcb_rm.results == 'away') | (
                (fcb_rm['home name'] == team2) & (fcb_rm.results == 'home')))
    fcb_rm_draw = fcb_rm.where(fcb_rm.results == 'draw')

    labels = [team1, team2, 'Draw']
    sizes = [fcb_won.count(), rm_won.count(), fcb_rm_draw.count()]

    fig4 = go.Figure(data=[go.Pie(labels=labels, values=sizes)])
    fig4.update_layout(
        title_text=f"{team1} v/s {team2} Face-off")
    fig4.show()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Visitors') \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/cmpt_732") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/cmpt_732") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    team1 = sys.argv[1]
    team2 = sys.argv[2]

    main(team1, team2)
