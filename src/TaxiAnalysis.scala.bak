package DataAnalysis

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object TaxiAnalysis extends App{
  
  println("Program Starts..")
  Logger.getLogger("org").setLevel(Level.ERROR)

   val spark = SparkSession.builder()
  .appName("Taxi_rides")
  .master("local[2]")
  .enableHiveSupport()
  .getOrCreate()
  
   var taxiRidesDF = spark.read
  .format("parquet")
  .option("header", true) 
  .option("path", "D:/Projects/Dataset/Taxi/yellow_taxi_trip_records/2022/yellow*.parquet")
  .load()
  
  val totalRecords = taxiRidesDF.select(expr("count (*)")).as("Total Records").show()
  
   var zoneDF = spark.read
  .format("csv")
  .option("header", true) 
  .option("path", "D:/Learning/BigData/Workspace/Spark/Project/data/taxi_zone_lookup.csv")
  .load()
  
  var rideData = taxiRidesDF.where(expr("year(tpep_pickup_datetime) = 2022")).orderBy("tpep_pickup_datetime").
    withColumn("refNo", monotonically_increasing_id()+1).
    select("refNo", "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
      "passenger_count", "trip_distance", "PULocationID",
      "DOLocationID", "payment_type", "fare_amount", "total_amount")
  
//1.Hours that has highest no of taxi rides started
    println("1.Hours that has highest no of taxi rides started")
    var ridesPerHour = rideData.groupBy(expr("hour(tpep_pickup_datetime)")).count()
    ridesPerHour.sort(desc("count"))
    .show()
//2.which area has highest taxi rides started
//  --> Highest rides from
    println("2.1 Highest rides from")
    val temp = rideData.groupBy("PULocationID").count().sort(desc("count"))
    val ridesFrom = temp.join(zoneDF, temp.col("PULocationID") === zoneDF.col("LocationID"), "inner").drop("service_zone")
    .show()
//  --> Highest rides to
    println("2.2 Highest rides to")
    val temp1 = rideData.groupBy("DOLocationID").count()sort(desc("count"))
    val ridesTo = temp1.join(zoneDF, temp1.col("DOLocationID") === zoneDF.col("LocationID"), "inner").drop("service_zone")
 .show()
 
//3. Avg fair
    println("3. Avg fair")
    val avgFair = rideData.agg(avg("total_amount")).as("Average Fair")
    .show()
    
//4. Trip distance
    println("4. Trip distance")
    val avgTripDistance = rideData.agg(avg("trip_distance")).as("Average Trip Distance").show()
    
    
//5. Vendor ratio
    println("5. Vendor ratio")
    rideData.createOrReplaceTempView("rideDf")
    val vendorsData = spark.sql("""
      select VendorID, count(*) as totalRides
      from rideDF
      group by VendorID
      """).show()

}