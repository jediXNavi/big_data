from pyspark import SparkContext, SparkConf
import sys
assert sys.version_info >= (3,5)
import json

inputs = sys.argv[1]
output = sys.argv[2]

def format(dict):
    return dict['subreddit'], (dict['score'], 1)

def json_parser(line):
    parser = json.loads(line, object_hook=format)
    return parser

def serialize(line):
    encoder = json.dumps(line, sort_keys=True)
    return encoder

def reducer(x, y):
    sum_scores = x[0] + y[0]
    count = x[1] + y[1]
    return sum_scores, count

def cal_avg(kv):
    k, (sum_score, count) = kv
    return (k, sum_score / count)

def main(inputs,output):

    rdd0 = sc.textFile(inputs)
    rdd1 = rdd0.map(json_parser)
    rdd2 = rdd1.reduceByKey(reducer)
    rdd3 = rdd2.map(cal_avg)
    back_to_json = rdd3.map(serialize)
    back_to_json.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.4'
    sc.setLogLevel('WARN')
    main(inputs, output)



