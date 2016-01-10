from pyspark import SparkContext

spark = SparkContext()
text = spark.textFile("hdfs://cluster-1-m/user/ic/enwiki.xml")
wordcount = text.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
wordcount.saveAsTextFile("hdfs://cluster-1-m/user/ic/enwiki-pyspark-out.txt")
