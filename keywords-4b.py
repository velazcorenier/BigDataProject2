from pyspark.sql import SQLContext
from pyspark.sql import Row, SparkSession
from stop_words import get_stop_words
stop_words = get_stop_words('en')
stop_words.extend(get_stop_words('spanish'))
stop_words.append("rt")
sqlContext = SQLContext(sc)
rdd = sqlContext.read.json('hdfs://localhost:9000/user/renier/tweets.json')
records=rdd.collect()
#records = [element for element in records if "delete" not in element] #remove delete tweets
records = [element["text"] for element in records if "text" in element]
rdd = sc.parallelize(records)
rdd = rdd.filter(lambda x: len(x) > 2 )
rdd = rdd.flatMap(lambda x: x.split()).map(lambda x: x.lower())
rdd = rdd.filter(lambda x: x not in stop_words)
rdd = rdd.filter(lambda x: x.startswith(('@','#','RT','--','&', 'https','"',':')) == False )
rdd = rdd.filter(lambda x: len(x) > 1 )
keywordDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(keyword=x)))
keywordDataFrame.createOrReplaceTempView("keywords")
keywordDataFrame = spark.sql("select keyword, count(*) as total from keywords group by keyword order by total desc")
#keywordDataFrame.show()
keywordDataFrame.repartition(1).write.csv('/home/osboxes/Desktop/BigDataProject2/keywords.csv', sep = '|')
