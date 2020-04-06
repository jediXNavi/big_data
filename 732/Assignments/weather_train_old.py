import sys

from pyspark.sql import SparkSession, functions, types
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer, StringIndexer
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def weather_train(input,model_path):
    tmax_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.DateType()),
        types.StructField('latitude', types.FloatType()),
        types.StructField('longitude', types.FloatType()),
        types.StructField('elevation', types.FloatType()),
        types.StructField('tmax', types.FloatType()),
    ])
    data = spark.read.csv(input,schema = tmax_schema)
    train, validation = data.randomSplit([0.75,0.25])
    train = train.cache()
    validation = validation.cache()

    sql_query = """SELECT latitude, longitude, elevation, dayofyear(date) AS dy, tmax FROM __THIS__"""
    transformer = SQLTransformer(statement=sql_query)
    assemble_features = VectorAssembler(inputCols=['latitude','longitude','elevation','dy'],outputCol='features')
    classifier = DecisionTreeRegressor(featuresCol='features',labelCol='tmax')
    weather_pipeline = Pipeline(stages=[transformer,assemble_features,classifier])
    model = weather_pipeline.fit(train)
    model.write().overwrite().save(model_path)

    prediction = model.transform(validation)
    #Scoring the model
    evaluator = RegressionEvaluator(predictionCol='prediction',labelCol='tmax',metricName='r2')
    score = evaluator.evaluate(prediction)
    print("Score of the weather model is",score)

if __name__ == '__main__':
    spark = SparkSession.builder.appName('weather_predictor').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    input = sys.argv[1]
    model_path = sys.argv[2]
    weather_train(input,model_path)