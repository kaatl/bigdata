from pyspark import SparkContext
from pyspark import SQLContext



if __name__ == "__main__":
    sc = SparkContext(appName="Test")
    sqlContext = SQLContext(sc)

    listings_textfile = sc.textFile("listings_us.csv")

    listings = listings_textfile.map(lambda x: tuple(x.split('\t')))
    listings_sample = listings.sample(False, 0.1, 7)

    print("WORD COUNT: ", listings_textfile.count())
    print("WORD COUNT SAMPLE: ", listings.count())

    listings_sample.coalesce(1).saveAsTextFile("sampleFiles.csv")

    sc.stop()
