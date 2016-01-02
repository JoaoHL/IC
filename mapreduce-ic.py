from pyspark import SparkContext

spark = SparkContext()
text = spark.textFile("hdfs://iniciacao-m:10000/user/ic/enwiki.xml")
wordcount = text.flatMap(lambda line: line.split(' ')).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
wordcount.saveAsTextFile("hdfs://iniciacao-m:10000/user/ic/out/enwiki-pyspark-out")
