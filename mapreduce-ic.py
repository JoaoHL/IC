import pyspark

spark = pyspark.SparkContext()
text = spark.textFile("hdfs://iniciacao-m:9090/user/hadoop/enwiki-samplefile.xml")
counts = text.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("hdfs://iniciacao-m:9090/user/hadoop/enwiki-samplefile-procd.txt")
