from pyspark import SparkContext
from pyspark import SQLContext

sc = SparkContext(appName="Test")
sqlContext = SQLContext(sc)

words = sc.textFile("/Users/Kathrine/Desktop/test1.txt")
words.count()
print(words.count())
