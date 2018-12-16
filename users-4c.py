from pyspark.sql import SQLContext
from pyspark.sql import Row, SparkSession
sqlContext = SQLContext(sc)
rdd = sqlContext.read.json('hdfs://localhost:9000/user/renier/tweets.json')
records=   rdd.collect()
records = [element["user"]["name"] for element in records if "user" in element] 
rdd = sc.parallelize(records)
rdd = rdd.filter(lambda x: x.startswith(('@','#','RT','--','https','"',':', '.', ' ')) == False )
rdd = rdd.filter(lambda x:len(x) > 2 )
if rdd.count() > 0:
    userDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(idParticipant=x)))
    userDataFrame.createOrReplaceTempView("ids")
    userDataFrame = spark.sql("select idParticipant, count(*) as total from ids group by idParticipant order by total desc")
    userDataFrame.show()
    userDataFrame.repartition(1).write.csv('/home/osboxes/Desktop/BigDataProject2/users.csv', sep = '|')

