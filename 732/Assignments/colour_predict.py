import sys

from pyspark.sql import SparkSession, functions, types
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import VectorAssembler, SQLTransformer,StringIndexer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions

def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    #To convert R,G,B to LabCIE
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    sql_transformed = SQLTransformer(statement=rgb_to_lab_query)

    rgb_assembler = VectorAssembler(inputCols=['R','G','B'],outputCol='features')
    lab_assembler = VectorAssembler(inputCols=['labL','labA','labB'],outputCol='features')

    word_indexer = StringIndexer(inputCol='word',outputCol='indexed')
    classifier = MultilayerPerceptronClassifier(labelCol='indexed',layers=[3, 30, 11])

    rgb_pipeline = Pipeline(stages=[rgb_assembler,word_indexer,classifier])
    lab_pipeline = Pipeline(stages=[sql_transformed,lab_assembler,word_indexer,classifier])

    rgb_model = rgb_pipeline.fit(train)
    lab_model = lab_pipeline.fit(train)

    prediction = rgb_model.transform(validation)
    prediction_lab = lab_model.transform(validation)
    prediction.show()
    prediction_lab.show()

    #Testing the model
    evaluator = MulticlassClassificationEvaluator(predictionCol='prediction',labelCol='indexed',metricName='f1')
    lab_evaluator = MulticlassClassificationEvaluator(predictionCol='prediction',labelCol='indexed',metricName='f1')
    score = evaluator.evaluate(prediction)
    lab_score = lab_evaluator.evaluate(prediction_lab)
    plot_predictions(rgb_model, 'RGB', labelCol='word')
    plot_predictions(lab_model, 'LAB', labelCol='word')
    print('Validation score for RGB model: %g' % (score, ))
    print('Validation score for LAB model:', lab_score)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Color_predictor').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    inputs = sys.argv[1]
    main(inputs)

