from pyspark import SparkConf, SparkContext
import sys
import re, string

def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    for w in wordsep.split(line.lower()):
        yield (w, 1)

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs,output):
    text = sc.textFile(inputs)
    words = text.flatMap(words_once)
    wordcount = words.reduceByKey(add)
    words_nonempty = wordcount.filter(lambda x: len(x[0]) > 0)
    outdata = words_nonempty.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':

    conf = SparkConf().setAppName('word count improved')
    sc = SparkContext(conf=conf)
    assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+

    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)