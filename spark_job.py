import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("spark://43.240.98.163:7077").setAppName('testpython')
SparkContext.setSystemProperty('spark.executor.cores', '80')
SparkContext.setSystemProperty('spark.executor.memory', '35')
sc = SparkContext(conf=conf)
sc.parallelize([0, 2, 3, 4, 6], 5).glom().collect()
sc.stop()
