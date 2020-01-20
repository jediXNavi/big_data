from pyspark import SparkContext, SparkConf
import sys

inputs = sys.argv[1]
output = sys.argv[2]
conf = SparkConf().setAppName('wikipedia-popular')
sc = SparkContext(conf=conf)


def tuple_outta_stats(line):
    wiki_tuple = tuple(line.split(" "))
    yield (wiki_tuple)

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

def get_key(kv):
    return kv[0]

def reduce(x, y):
    if (max(x[0], y[0]) == x[0]):
        return x
    else:
        return y

# def reduce_new(x,y):
#     if(str(x[0])>str(y[0])):
#         return x
#     elif(str(x[0])==str(y[0])):
#         return x,y
#     else:
#         return y

def langFilter(line):
    if line[1] == "en":
        return (line)

def isBoring(line):
    return not (line[2].startswith(("Main_Page", "Special:")))


text = sc.textFile(inputs)
wiki_map = text.flatMap(tuple_outta_stats)
wiki_int = wiki_map.map(lambda x: (x[0], x[1], x[2], int(x[3]), x[4]))
wiki_langfilter = wiki_int.filter(langFilter)
wiki_boringpages = wiki_langfilter.filter(isBoring)
wiki_kv_old = wiki_boringpages.map(lambda x: (x[0], x[3]))
wiki_kv = wiki_boringpages.map(lambda x: (x[0], (x[3], x[2])))
max_count = wiki_kv.reduceByKey(reduce)

max_count.map(tab_separated).sortBy(get_key).saveAsTextFile(output)