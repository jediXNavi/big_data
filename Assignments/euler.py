from pyspark import SparkContext, SparkConf
import random
import sys

def main(sample):

    rdd0 = sc.parallelize(range(1,int(sample)),numSlices=10)
    rdd1 =rdd0.map(deriving_eulers)
    euler_rdd = rdd1.reduce(lambda x,y:(x+y))
    euler_constant = float(euler_rdd)/int(sample)

    print('The approximate value of the constant is ', euler_constant)

def deriving_eulers(line):
    iter = 0
    sum = 0.0
    random.seed()
    while sum < 1:
          sum += random.random()
          iter += 1
    return iter


if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sample = sys.argv[1]
    main(sample)
