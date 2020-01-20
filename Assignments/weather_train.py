import sys

from pyspark.sql import SparkSession, functions, types
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer, StringIndexer
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def model_train(input,model_path):
    tmax_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.DateType()),
        types.StructField('latitude', types.FloatType()),
        types.StructField('longitude', types.FloatType()),
        types.StructField('elevation', types.FloatType()),
        types.StructField('tmax', types.FloatType()),
    ])
    data = spark.read.csv(input,schema= tmax_schema)
    train, validation = data.randomSplit([0.75,0.25])
    train = train.cache()
    validation = validation.cache()

    sql_query = """SELECT today.latitude, today.longitude, today.elevation, dayofyear(today.date) AS dy,yesterday.tmax AS yesterday_tmax, today.tmax
                     FROM __THIS__ as today
               INNER JOIN __THIS__ as yesterday
                       ON date_sub(today.date, 1) = yesterday.date
                      AND today.station = yesterday.station"""
    transformer = SQLTransformer(statement=sql_query)
    assemble_features = VectorAssembler(inputCols=['latitude','longitude','elevation','dy','yesterday_tmax'],outputCol='features')
    regressor = DecisionTreeRegressor(featuresCol='features',labelCol='tmax')
    weather_pipeline = Pipeline(stages=[transformer,assemble_features,regressor])
    model = weather_pipeline.fit(train)
    model.write().overwrite().save(model_path)

    prediction = model.transform(validation)
    #Scoring the model
    evaluator = RegressionEvaluator(predictionCol='prediction',labelCol='tmax',metricName='rmse')
    score = evaluator.evaluate(prediction)
    print("Score of the weather model is",score)

if __name__ == '__main__':
    spark = SparkSession.builder.appName('weather_predictor_trainer').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    input = sys.argv[1]
    model_path = sys.argv[2]
    model_train(input,model_path)