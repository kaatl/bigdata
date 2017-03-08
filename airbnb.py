from pyspark import SparkContext
from pyspark import SQLContext

import warnings

def task2(listings):
    print
    print "********************* Task 2b *********************"
    print "Distinct values for one column"
    # Ha en for-loop som gjor dette for hver kolonne? Tar bare en naa
    # Distinct er casesensitive
    print "Number of distinct values: ", listings.map(lambda row: row[0]).distinct().count()

    print
    print "********************* Task 2c *********************"
    cities = listings.map(lambda row: row[15]).distinct()
    print "Cities count: ", cities.count()

    for city in cities.collect():
        print city

    print
    print "********************* Task 2d *********************"
    #Find the higest monthly price, this is not working as it should
    highestMontlyPrice = listings.map(lambda row: row[59].split()).reduce(lambda a, b: a if (a > b) else b)
    print "Highest montly price:", highestMontlyPrice

    #Count country
    country = listings.map(lambda row: row[17]).distinct()
    print "Country count: ", country.count()

    for country in country.collect():
        print country

if __name__ == "__main__":
    sc = SparkContext(appName="AirBnb")
    sc.setLogLevel("WARN")
    listings_textfile = sc.textFile("listings_us.csv")
    header = listings_textfile.first() #extract header
    listings_textfile  = listings_textfile.filter(lambda row: row != header) #ignores the header
    listings = listings_textfile.map(lambda x: tuple(x.split('\t')))
    listings_sample = listings.sample(False, 0.1, 7)
    # Lagre sample:
    # listings_sample.coalesce(1).saveAsTextFile("sampleFiles.csv")

    task2(listings_sample)

    sc.stop()
