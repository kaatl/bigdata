from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, lower


def task2b(listings, header):
    print
    print "********************* Task 2b *********************"
    print "Number of distinct values in each columns:"

    # Printer antall distinct values for hver kolonne, er driiiiiiiittregt
    for i in range(len(header)):
        print i, " ", header[i], " - ", listings.map(lambda row: row[i]).distinct().count()

def task2(listings):

    print
    print "********************* Task 2c *********************"

    cities = listings.select(col("cities")).distinct()
    print "Cities count: ", cities.count()

    citiesLength = cities.count()

    # The arguments in "show()" just specifies how many items it should show, and that it should not be truncated
    cities.show(citiesLength, truncate = False)

    print
    print "********************* Task 2d *********************"

    countries = listings.select(col("countries")).distinct()
    countriesLength = countries.count()

    # Show number of countries
    print "Number of countries are: ", countriesLength

    # Show the list of countries
    print "The contries that are mentioned are"
    countries.show(countriesLength, truncate = False)


    monthly_prices = listings.select(col("monthly_prices")).distinct().dropna(None) #Drop values that are "None"

    #Show max price per month
    print "Highest monthly price "
    listings.agg({"monthly_prices":"max"}).show()

    #Show min price per month
    print "Lowest monthly price "
    listings.agg({"monthly_prices":"min"}).show()

    #Show the monthly price sorted min to max
    #print "Sorted monthly price ", sorted(monthly_prices.collect())

    #Shows average monthly price
    listings.agg({"monthly_prices":"avg"}).show()

def task3(listings):
    print
    print '********************* Task 3a *********************'
    print "For each city, the average booking price per night is: "
    cityAvgPrice = listings.groupBy("cities").agg({"price":"avg"}).orderBy("cities")
    cityAvgPrice.show(cityAvgPrice.count(), truncate = False)

    print
    print '********************* Task 3b *********************'
    print "Average booking price per room type per night: "
    roomType = listings.groupBy("roomType","cities").agg({"price":"avg"}).orderBy("cities")
    roomType.show(roomType.count(), truncate = False)

    print
    print "********************* Task 3c *********************"
    reviewsMonthAvg = listings.groupBy("cities").agg({"reviewsPerMonth":"avg"}).orderBy("cities")
    reviewsMonthAvg.show(reviewsMonthAvg.count(), truncate = False)

    print
    print "********************* Task 3d *********************"
    print "Estimated number of nights booked per year: "
    #Use number of reviews per month field in listings and assume that
    #only 70% of customers leave a review and on average they spend 3
    #nights per booking.

    #Number of nigths booked per year per city
    NumberOfNightsBooked = listings.withColumn("reviewsPerMonth", listings["reviewsPerMonth"]/0.7*3*12)
    NumberOfNights = NumberOfNightsBooked.groupBy("cities").agg({"reviewsPerMonth":"sum"}).orderBy("cities")
    NumberOfNights.show(NumberOfNights.count(), truncate = False)

    #listings = listings.withColumn("reviewsPerMonth", listings["reviewsPerMonth"]*3 + (listings["reviewsPerMonth"]*3 / 70) * 30)
    #reviewsMonthTotal = listings.groupBy("cities").agg({"reviewsPerMonth":"sum"}).orderBy("cities")
    #reviewsMonthTotal.show(reviewsMonthTotal.count(), truncate = False)

    print
    print "********************* Task 3e *********************"
    print "Estimated amount of money spent on AirBnB accomodation per year: "
    #Number of nigths booked per year per city
    NumberOfNightsBooked = listings.withColumn("reviewsPerMonth", listings["reviewsPerMonth"]/0.7*3*12 + (listings["price"]*12))
    AmountOfMoney = NumberOfNightsBooked.groupBy("cities").agg({"reviewsPerMonth":"sum"}).orderBy("cities")
    AmountOfMoney.show(AmountOfMoney.count(), truncate = False)


def task4(listings):
    print
    print "********************* Task 4a *********************"
    print "Print global average number of listing per host: "
    listingPerHost = listings.groupBy("hostID").agg({"listingID":"count"}).orderBy("hostID")
    listingPerHost.show(listingPerHost.count(), truncate = False)

    hostsWithOverOneListing = listings.groupBy("hostID").count().where("count(listingID) > 1").orderBy("hostID")
    #hostsWithOverOneListing.show(hostsWithOverOneListing.count(), truncate = False)

    totalListingPerHost = listingPerHost.count()
    totalListingMoreThanOne = hostsWithOverOneListing.count()


    print "********************* Task 4b *********************"
    print "Percentage of hosts with more than 1 listings are: ",  float(totalListingMoreThanOne) / float(totalListingPerHost) * 100.0, " %"



if __name__ == "__main__":
    sc = SparkContext(appName="AirBnb")
    sqlContext = SQLContext(sc)

    sc.setLogLevel("WARN")
    listings_textfile = sc.textFile("listings_us.csv")
    calendar_textfile = sc.textFile("calendar_us.csv")

    header = listings_textfile.first() #extract header
    print
    header = header.split()
    #for x in range(len(header)):
        #print x, " - ", header[x]

    listings_textfile = listings_textfile.filter(lambda row: row != header) #ignores the header
    calendar_textfile = calendar_textfile.filter(lambda row: row != header) #ignores the header

    listings = listings_textfile.map(lambda x: tuple(x.split('\t')))
    calendar = calendar_textfile.map(lambda x: tuple(x.split('\t')))

    listings = listings.sample(False, 0.1, 7) # Sample
    calendar = calendar.sample(False, 0.1, 7) # Sample


    # Lagre sample:
    #listings.coalesce(1).saveAsTextFile("sampleFiles.csv")
    #calendar.coalesce(1).saveAsTextFile("calendar_sampleFiles.csv")


    listings_df = listings.map(
        lambda c: Row(
            cities = c[15].lower().strip(),
            countries = c[17].lower().strip(),
            hostID = c[28],
            listingID = [43],
            monthly_prices = c[59].replace(',','').replace('$',''),
            price = c[65].replace(',','').replace('$',''),
            reviewsPerMonth = c[80],
            roomType = c[81],
            accomodation = c[1]
        ))



    calendar_df = calendar.map(
        lambda c: Row(
            listingID = c[0],
            date = c[1],
            available = c[2]
        ))


    listings_df = sqlContext.createDataFrame(listings_df)
    calendar_df = sqlContext.createDataFrame(calendar_df)

    listings_df = listings_df.withColumn('monthly_prices', listings_df['monthly_prices'].cast(DoubleType()))



    #task2b(listings, header)
    #task2(listings_df)
    task3(listings_df)
    #task4(listings_df)

    sc.stop()
