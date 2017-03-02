from pyspark import SparkContext
from pyspark import SQLContext



if __name__ == "__main__":
    sc = SparkContext(appName="Test")
    sqlContext = SQLContext(sc)

    # words = sc.textFile("listings_us.csv")

    words = sc.textFile("test1.txt")
    print("WORD COUNT: ", words.count())

    sc.stop()
