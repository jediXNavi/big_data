import sys
import datetime

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType())
])

def test_model(model_file):
    # get the data
    current_date = datetime.datetime.strptime('2019-11-08', '%Y-%m-%d')
    target_date = datetime.datetime.strptime('2019-11-09', '%Y-%m-%d')
    weather_data = spark.createDataFrame([('SFU Campus',current_date,49.2771,-122.9146,330.0,12.0),('SFU Campus',target_date,49.2771,-122.9146,330.0,12.0)],schema=tmax_schema)

    # load the model
    model = PipelineModel.load(model_file)

    # use the model to make predictions
    predictions = model.transform(weather_data)
    predictions = predictions.select('prediction').collect()[0][0]

    print('Predicted tmax tomorrow:', predictions)


if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)