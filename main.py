from pyspark import SparkContext

sc=SparkContext("yarn","spark_study")
data=sc.textFile("hdfs:///user/root/data/text")
words_rdd=data.flatMap(lambda x: x.split(" "))
words_count_rdd=words_rdd.map(lambda x: (x,1))
count_rdd=words_count_rdd.reduceByKey(lambda x,y: x+y)
print(count_rdd.collect())