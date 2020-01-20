from pyspark import SparkContext, SparkConf
import sys
import json
import re

def main(inputs, output):
    rdd0 = sc.textFile(inputs)
    rdd1= rdd0.map(json_parser)
    rdd2= rdd1.flatMap(filter_data).cache()
    rdd_pos_scores = rdd2.filter(lambda x: x[1] > 0)
    rdd_neg_scores = rdd2.filter(lambda x: x[1] <= 0)
    rdd_pos_scores.map(serialize).saveAsTextFile(output + '/positive')
    rdd_neg_scores.map(serialize).saveAsTextFile(output + '/negative')

def format(dict):
    return dict['subreddit'],dict['score'],dict['author']

def json_parser(line):
    parser = json.loads(line,object_hook=format)
    return parser

def filter_data(line):
    if 'e' in line[0]:
        yield line


def serialize(line):
    encoder = json.dumps(line)
    return encoder

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]

    conf = SparkConf().setAppName('reddit_etl')
    sc = SparkContext(conf=conf)
    main(inputs, output)




