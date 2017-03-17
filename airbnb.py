from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import Row, SQLContext
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import col, lower, desc


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


def task4(listings, list_cal_joined_df):
    print
    print "********************* Task 4a *********************"
    print "Print global average number of listing per host: "
    #listingPerHost = listings.groupBy("hostID").agg({"listingID":"count"}).orderBy("hostID")
    #listingPerHost.show(listingPerHost.count(), truncate = False)

    #hostsWithOverOneListing = listings.groupBy("hostID").count().where("count(listingID) > 1").orderBy("hostID")
    ##hostsWithOverOneListing.show(hostsWithOverOneListing.count(), truncate = False)

    #totalListingPerHost = listingPerHost.count()
    #totalListingMoreThanOne = hostsWithOverOneListing.count()

    print
    print "********************* Task 4b *********************"
    #print "Percentage of hosts with more than 1 listings are: ",  float(totalListingMoreThanOne) / float(totalListingPerHost) * 100.0, " %"

    print
    print "********************* Task 4c *********************"
    #For each city, find top 3 hosts with the highest income (throughout
    #the whole time of the dataset). Calculate the estimated income based
    #on the listing price and number of days it was booked according to
    #the calendar dataset.
    cities = listings.select(col("cities")).distinct().collect()

    join_filtered = list_cal_joined_df.filter(list_cal_joined_df.available == "f")
    topThreeHosts = join_filtered.groupBy("cities", "hostID").agg({"price":"sum"})
    topThreeHosts = topThreeHosts.sort("cities", desc("sum(price)"))
    topThreeHosts.na.drop()

    for city in cities:
        print city
        topThreeHosts.filter(topThreeHosts.cities == city[0]).limit(3).show()


def task5(review_listings_joined_df, listings):
    print
    print "********************* Task 5a *********************"
    print "Top 3 guests ranked by their number of bookings: "
    #city = listings.select(col('cities')).distinct().collect()
    #review_listings_joined_df.na.drop()
    #joineddf = review_listings_joined_df.groupBy('cities', 'reviewID').agg({"reviewID":"count"}).sort(desc('count(reviewID)')).cache()

    #for cities in city:
        #joineddf.filter(joineddf.cities == cities.cities).limit(3).show()


    print
    print "********************* Task 5b *********************"
    print "The guest who spent the most money on accomodation: "
    review_listings_joined_df = review_listings_joined_df.filter(review_listings_joined_df.reviewID != "")
    review_listings_joined_df.groupBy('reviewID').agg({'price':'sum'}).sort(desc('sum(price)')).limit(1).show()



if __name__ == "__main__":
    sc = SparkContext(appName="AirBnb")
    sqlContext = SQLContext(sc)

    sc.setLogLevel("WARN")
    listings_textfile = sc.textFile("listings_us.csv")
    calendar_textfile = sc.textFile("calendar_us.csv")
    reviews_textfile = sc.textFile("reviews_us.csv")

    header = listings_textfile.first() #extract header
    calendarHeader = calendar_textfile.first()
    reviewsHeader = reviews_textfile.first()



    #listings_textfile = listings_textfile.filter(lambda row: row != header) #ignores the header
    #calendar_textfile = calendar_textfile.filter(lambda row: row != header) #ignores the header

    listings = listings_textfile.filter(lambda row: row != header).map(lambda x: tuple(x.split('\t')))
    calendar = calendar_textfile.filter(lambda row: row != calendarHeader).map(lambda x: tuple(x.split('\t')))
    reviews = reviews_textfile.filter(lambda row: row != reviewsHeader).map(lambda x: tuple(x.split('\t')))

    print
    #header = header.split()
    #for x in range(len(header)):
        #print x, " - ", header[x]

    print
    #reviewsHeader  = reviewsHeader.split()
    #for x in range(len(reviewsHeader)):
        #print x, " - ", reviewsHeader[x]

    listings = listings.sample(False, 0.1, 7) # Sample
    calendar = calendar.sample(False, 0.1, 7) # Sample
    reviews = reviews.sample(False, 0.1, 7) # Sample


    # Lagre sample:
    #listings.coalesce(1).saveAsTextFile("sampleFiles.csv")
    #calendar.coalesce(1).saveAsTextFile("calendar_sampleFiles.csv")
    #reviews.coalesce(1).saveAsTextFile("reviews_sampleFiles.csv")


    listings_df = listings.map(
        lambda c: Row(
            cities = c[15].lower().strip(),
            countries = c[17].lower().strip(),
            hostID = c[28],
            listingID = c[43],
            monthly_prices = c[59].replace(',','').replace('$',''),
            price = c[65].replace(',','').replace('$',''),
            reviewsPerMonth = c[80],
            roomType = c[81]
        ))

    calendar_df = calendar.map(
        lambda c: Row(
            listingID = c[0],
            date = c[1],
            available = c[2]
        ))

    reviews_df = reviews.map(
        lambda c: Row(
            listingID = c[0],
            reviewID = c[3]
        ))


    listings_df = sqlContext.createDataFrame(listings_df)
    calendar_df = sqlContext.createDataFrame(calendar_df)
    reviews_df = sqlContext.createDataFrame(reviews_df)

    listings_df = listings_df.withColumn('monthly_prices', listings_df['monthly_prices'].cast(DoubleType()))

    listings_df = listings_df.withColumn('listingID', listings_df['listingID'].cast(IntegerType()))
    calendar_df = calendar_df.withColumn('listingID', calendar_df['listingID'].cast(IntegerType()))
    reviews_df = reviews_df.withColumn('listingID', reviews_df['listingID'].cast(IntegerType()))

#    listings_df.na.drop()
#    calendar_df.na.drop()

    listings_calendar_joined_df = listings_df.join(calendar_df, 'listingID', "outer")
    review_listings_joined_df = listings_df.join(reviews_df, 'listingID', "outer")


    #task2b(listings, header)
    #task2(listings_df)
    #task3(listings_df)
    #task4(listings_df, listings_calendar_joined_df)
    task5(review_listings_joined_df,listings_df)

    sc.stop()
