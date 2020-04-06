from pyspark import SparkContext, SparkConf
import sys
assert sys.version_info >= (3,5)
import json

inputs = sys.argv[1]
output = sys.argv[2]

def main(inputs,output):

    rdd0 = sc.textFile(inputs)
    rdd1 = rdd0.map(json_parser).cache()
    commentbysub = rdd1.map(lambda c: (c['subreddit'], c))
    rdd2 = rdd1.map(lambda line: (line['subreddit'], (line['score'], 1)))
    rdd3 = rdd2.reduceByKey(reducer)
    rdd4 = rdd3.map(cal_avg)
    filtered_rdd4 = rdd4.filter(lambda line: line[1] > 0)
    pairRDD = filtered_rdd4.join(commentbysub)
    finalRDD = pairRDD.map(lambda line: (line[1][1]['author'], (line[1][0], line[1][1]['score'])))
    finalRDD1 = finalRDD.map(rel_score)
    finalRDD2 = finalRDD1.reduceByKey(max_score)
    finalRDD2.sortBy(get_key, ascending=False).saveAsTextFile(output)

def json_parser(line):
    parser = json.loads(line)
    return parser

def serialize(line):
    encoder = json.dumps(line, sort_keys = True)
    return encoder

def reducer(x,y):
    sum_scores = x[0] + y[0]
    count = x[1] + y[1]
    return sum_scores,count

def cal_avg(kv):
    k, (sum_score, count) = kv
    return (k, sum_score/count)

def rel_score(kv):
    k, (avg, comm_score) = kv
    return k,comm_score/avg

def max_score(x,y):
    if y>x:
        return y
    else:
        return x

def get_key(kv):
    return kv[1]

if __name__ == '__main__':
    conf = SparkConf().setAppName('rel_score').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.4'
    sc.setLogLevel('WARN')
    main(inputs, output)



