from pyspark import SparkContext

spark = SparkContext()

# text = spark.textFile("hdfs://iniciacao-m:10000/user/ic/files.py")

text = spark.textFile("file:///usr/lib/spark/python/pyspark/files.py")
wordcount = text.flatMap(lambda line: line.split(' ')).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)


# wordcount.saveAsTextFile("hdfs://iniciacao-m:10000/user/ic/out/files-py-procd")
wordcount.saveAsTextFile("file:///usr/lib/spark/data/filespy-procd")
