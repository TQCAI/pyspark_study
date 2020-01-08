from pyspark import SparkContext

sc=SparkContext("yarn","spark_study")


from pathlib import Path

def fn(x):
     return Path("/etc/hostname").read_text()

lst=[1 for i in range(100000)]
rdd1=sc.parallelize(lst)
rdd2=rdd1.map(fn)
rdd3=rdd2.map(lambda x:(x,1))
rdd4=rdd3.reduceByKey(lambda x,y:x+y)
rdd4.collect()
