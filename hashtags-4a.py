from pyspark.sql import SQLContext
from pyspark.sql import Row, SparkSession
sqlContext = SQLContext(sc)
rdd = sqlContext.read.json('hdfs://localhost:9000/user/renier/tweets.json')
records=   rdd.collect()
records = [element for element in records if "delete" not in element] #remove delete tweets
records = [element["entities"]["hashtags"] for element in records if "entities" in element] #select only hashtags part
records = [x for x in records if x] #remove empty hashtags
records = [element[0]["text"] for element in records]
rdd = sc.parallelize(records)
hashtagsDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(hashtag=x)))
hashtagsDataFrame.createOrReplaceTempView("hashtags")
hashtagsDataFrame = spark.sql("select hashtag, count(*) as total from hashtags group by hashtag order by total")
#hashtagsDataFrame.show()
hashtagsDataFrame.repartition(1).write.csv('/home/osboxes/Desktop/BigDataProject2/hashtags.csv', sep = '|')

