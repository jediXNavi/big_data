import re
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import types, SparkSession, functions, types
import numpy as np
import pandas as pd
# import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.figure_factory as ff
import plotly.graph_objects as go

def main():
    players_data = spark.read.format("mongo").options(collection='players').load()
    players2 = spark.read.format("mongo").options(collection='players2').load()
    cc = spark.read.format("mongo").options(collection='countryContinent').load()

    cc = cc.select('country', 'continent')

    ## 2 columns missing nationality and body type

    players_a = players2.select('age', 'height_cm', 'weight_kg', 'nationality', 'club', 'overall', 'potential', 'body_type', 'pace', 'physic', 'movement_agility', 'power_stamina', 'mentality_aggression')
    # 'age','height','weight','nationality','club',body type','overall rating','potential','pace','physique','movement      agility','stamina','aggression'
    nationalities_agg = players2.select('nationality').distinct()
    nationalities_agg = nationalities_agg.join(cc, nationalities_agg['nationality'] == cc['country']).drop('country')
    players_a = players_a.join(nationalities_agg, players_a['nationality'] == nationalities_agg['nationality']).drop('nationality')
    nationalities_agg = players_a.select('continent').distinct()
    l = nationalities_agg.collect()
    country_list = {}
    for i in range(0, len(l)):
        country_list.update({l[i][0]: str(i + 1)})
    # country_list

    players_a = players_a.withColumn('body_type', \
                                     functions.when((functions.col('body_type') == 'Lean'), 3) \
                                     .when((functions.col('body_type') == 'Normal'), 2) \
                                     .when((functions.col('body_type') == 'Stocky'), 1) \
                                     .otherwise(0))


    players_a = players_a.replace(country_list, 1, 'continent')
    players_a.show(3)

    players_b = players_a.select(players_a['weight_kg'], players_a['continent'], players_a['club'])
    players_b = players_b.groupBy('continent').agg(functions.avg('weight_kg').alias('average weight')).orderBy('continent')
    players_c = players_a.select(players_a['continent'], players_a['body_type'], players_a['club'])
    players_c = players_c.groupBy('continent', 'body_type').agg(functions.count('body_type').alias('number of players')).orderBy('continent')
    players_c = players_c.withColumn('body_type', \
                                     functions.when((functions.col('body_type') == 3), 'Lean') \
                                     .when((functions.col('body_type') == 2), 'Normal') \
                                     .when((functions.col('body_type') == 1), 'Stocky') \
                                     .otherwise('null'))
    players_c = players_c.filter(players_c['body_type'] != 'null')

    players_d = players_a.select('age', 'club').groupBy('club').agg(functions.avg('age').alias('average_age')).orderBy(
        'average_age')
    players_d.show(truncate=False)

    players_a = players_a.toPandas()
    players_a.to_csv('cleaned.csv', index=False)

    inv_map = {v: k for k, v in country_list.items()}
    players_b = players_b.replace(inv_map, 1, 'continent')

    p = players_b.toPandas()
    fig = px.bar(p, x="average weight", y="continent", color="continent", orientation='h', height=400)
    fig.show()

    players_c = players_c.replace(inv_map, 1, 'continent')
    players_c.show()
    players_c = players_c.toPandas()


    fig = px.bar(players_c, x="number of players", y="continent", color="body_type", orientation='h', height=400)
    fig.show()

    # ax = sns.catplot(x="count", y="continent", hue="body_weight", kind="bar", data=players_c)
    # ax.set(xlabel='Number of Players', ylabel='Continent')

    # sphinx_gallery_thumbnail_number = 2

    df = pd.read_csv('cleaned.csv')
    df = df[1:]

    labels = ['age', 'height', 'weight', 'nationality', 'body type', 'overall rating', 'potential', 'pace', 'physique',
              'movement agility', 'stamina', 'aggression']

    corr = df.corr().to_numpy().round(2)

    fig, ax = plt.subplots(figsize=(8, 8))
    im = ax.imshow(corr)  # possible: jet

    fig = ff.create_annotated_heatmap(z=corr,
                                      annotation_text=corr,
                                      x=['age', 'height', 'weight', 'nationality', 'body type', 'overall rating',
                                         'potential', 'pace', 'physique',
                                         'movement agility', 'stamina', 'aggression'],
                                      y=['age', 'height', 'weight', 'nationality', 'body type', 'overall rating',
                                         'potential', 'pace', 'physique',
                                         'movement agility', 'stamina', 'aggression'],
                                      hoverongaps=False,
                                      colorscale='Viridis',
                                      hoverinfo='z'
                                      )
    fig.show()

    # We want to show all ticks...
    # ax.set_xticks(np.arange(len(labels)))
    # ax.set_yticks(np.arange(len(labels)))
    # ... and label them with the respective list entries
    # ax.set_xticklabels(labels)
    # ax.set_yticklabels(labels)

    # Rotate the tick labels and set their alignment.
    # plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
    #          rotation_mode="anchor")

    # Loop over data dimensions and create text annotations.
    # for i in range(len(labels)):
    #     for j in range(len(labels)):
    #         text = ax.text(j, i, corr[i, j],
    #                        ha="center", va="center", color="w")
    #
    # ax.set_title("Correlation Matrix")
    # fig.tight_layout()
    # plt.savefig('Downloads/myfig.png')
    # plt.show()

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Visitors') \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/cmpt_732") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/cmpt_732") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    main()