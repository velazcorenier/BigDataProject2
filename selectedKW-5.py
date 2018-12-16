from pyspark.sql import SQLContext
from pyspark.sql import Row, SparkSession
sqlContext = SQLContext(sc)
rdd = sqlContext.read.json('hdfs://localhost:9000/user/renier/tweets.json')
records=   rdd.collect()
records = [element["text"] for element in records if "text" in element]
rdd = sc.parallelize(records)
rdd = rdd.flatMap(lambda x: x.split()).map(lambda x: x.lower())
rdd = rdd.filter(lambda x: x == 'flu' or x == 'zika' or x == 'diarrhea' or x== 'ebola' or x== 'headache' or x== 'measles' or x == 'trump')
rdd = rdd.filter(lambda x: len(x) > 2 )
if rdd.count() > 0:
    keywordDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(keywordt=x)))
    keywordDataFrame.createOrReplaceTempView("keywordts")
    keywordDataFrame = spark.sql("select keywordt, count(*) as total from keywordts group by keywordt order by total desc")
    #keywordDataFrame.show()
    keywordDataFrame.repartition(1).write.csv('/home/osboxes/Desktop/BigDataProject2/selectedkeywords.csv', sep = '|')
