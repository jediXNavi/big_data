from pyspark.sql import SparkSession, functions, types
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def main():
    #Loading the data
    player_data = spark.read.format("mongo").options(collection='players2').load()

    #Conversion to integers - Mongo gets strings
    player_data = player_data.withColumn('age', player_data['age'].cast(types.IntegerType()))
    player_data = player_data.withColumn('weight_kg', player_data['weight_kg'].cast(types.IntegerType()))
    player_data = player_data.withColumn('overall', player_data['overall'].cast(types.IntegerType()))
    player_data = player_data.withColumn('pace', player_data['pace'].cast(types.IntegerType()))
    player_data = player_data.withColumn('passing', player_data['passing'].cast(types.IntegerType()))
    player_data = player_data.withColumn('physic', player_data['physic'].cast(types.IntegerType()))
    player_data = player_data.withColumn('movement_agility',
                                           player_data['movement_agility'].cast(types.IntegerType()))
    player_data = player_data.withColumn('power_stamina', player_data['power_stamina'].cast(types.IntegerType()))
    player_data = player_data.withColumn('mentality_aggression',
                                           player_data['mentality_aggression'].cast(types.IntegerType()))
    player_data = player_data.withColumn('shooting', player_data['shooting'].cast(types.IntegerType()))
    player_data = player_data.withColumn('dribbling', player_data['dribbling'].cast(types.IntegerType()))
    player_data = player_data.withColumn('defending', player_data['defending'].cast(types.IntegerType()))

    #Feature Engineering
    players_data1 = player_data.select('age', 'weight_kg', 'nationality', 'club', 'overall', 'potential', 'value_eur','wage_eur','movement_agility','power_stamina','mentality_aggression','pace','physic','passing', 'shooting', 'defending', 'dribbling')
    players_data1 = players_data1.dropna()
    players_data2 = players_data1.drop('club','wage_eur')
    players_data2 = players_data2.withColumn('value_range', \
                         functions.when((functions.col('value_eur').between(10000, 200000)), 1) \
                         .when((functions.col('value_eur').between(200000, 400000)), 2) \
                         .when((functions.col('value_eur').between(400000, 600000)), 3) \
                         .when((functions.col('value_eur').between(600000, 800000)), 4) \
                         .when((functions.col('value_eur').between(800000, 1000000)), 5) \
                         .otherwise(0))

    #ML
    train, validation = players_data2.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    feature_vector = VectorAssembler(
        inputCols=['age', 'weight_kg', 'overall', 'pace', 'passing', 'physic', 'movement_agility', 'power_stamina',
                   'mentality_aggression', 'passing', 'shooting', 'defending', 'dribbling'], outputCol='features')
    classifier = MultilayerPerceptronClassifier(layers=[13, 130, 6], featuresCol='features', labelCol='value_range',
                                                maxIter=500)
    ml_pipeline = Pipeline(stages=[feature_vector, classifier])
    model = ml_pipeline.fit(train)
    model.write().overwrite().save('wage_modeller')

    prediction = model.transform(validation)
    evaluator = MulticlassClassificationEvaluator(predictionCol='prediction',labelCol='value_range',metricName='f1')
    score = evaluator.evaluate(prediction)
    print('Validation score for new player wages: %g' % (score, ))

if __name__ == '__main__':

    spark =  SparkSession.builder.appName('player_wage_predictor') \
             .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/cmpt_732") \
             .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    main()