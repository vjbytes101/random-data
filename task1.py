#!/usr/bin/env python

from pyspark import SparkContext, SparkConf
from csv import reader
import sys

if __name__ == '__main__':
	conf = SparkConf().setAppName('app')
	sc = SparkContext(conf=conf)
	parking_violations = sc.textFile(sys.argv[1], 0)
	parking_violations = parking_violations.mapPartitions(lambda x: reader(x)).map(lambda x: (x[0],x[14],x[6],x[2],x[1])).map(lambda x: (x[0], list(x[1:])))
	open_violations = sc.textFile(sys.argv[2], 1)
	open_violations = open_violations.mapPartitions(lambda y: reader(y)).map(lambda y: (y[0],y[1])).map(lambda y: (y[0],list(y[1:])))
	ans = parking_violations.subtractByKey(open_violations).map(lambda x : x[0] + '\t' + ','.join(x[1]))
	ans.saveAsTextFile("task1.out")
	sc.stop()