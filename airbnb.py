from pyspark import SparkContext
from pyspark import SQLContext

def task2(listings):
    print "Task 2b"
    print "Distinct values for one column"
    # Ha en for-loop som gjor dette for hver kolonne? Tar bare en naa
    print "Number of distinct values: ", listings_sample.map(lambda row: row[0]).distinct().count()
    print

    print "Task 2c"
    cities = listings_sample.map(lambda row: row[15]).distinct()
    print "Cities count: ", cities.count()

    for city in cities.collect():
        print city

if __name__ == "__main__":
    sc = SparkContext(appName="AirBnb")

    listings_textfile = sc.textFile("listings_us.csv")
    listings = listings_textfile.map(lambda x: tuple(x.split('\t')))
    listings_sample = listings.sample(False, 0.1, 7)

    # Lagre sample:
    # listings_sample.coalesce(1).saveAsTextFile("sampleFiles.csv")

    task2(listings_sample)

    sc.stop()
