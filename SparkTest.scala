import org.apache.spark.sql.{SparkSession}

object SparkTest {
  def main(args: Array[String]) {

    //specifying input and output data directories
    val warehouseLocation = args(0) //input directory path to be fetched from argument 1
    val outputDir = args(1) //output directory path to be fetched from argument 2

    //Initializing Spark session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Nike Assignment")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()

    //Loading data from csv files into respective Data Frames
    val salesDF = spark.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true").load(warehouseLocation + "\\sales.csv")
    val storeDF = spark.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true").load(warehouseLocation + "\\store.csv")
    val productDF = spark.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true").load(warehouseLocation + "\\product.csv")
    val calendarDF = spark.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true").load(warehouseLocation + "\\calendar.csv")

    //Joining data into a single Data Frame
    val allSalesDF = salesDF
      .join(productDF, salesDF("productId") === productDF("productid"), "left")
      .join(storeDF, salesDF("storeId") === storeDF("storeid"), "inner")
      .join(calendarDF, salesDF("dateId") === calendarDF("datekey"), "inner")
      .cache()

    //Registering temporary view to perform SQL operations on data
    allSalesDF.createOrReplaceTempView("sales")

    //Aggregating data and creating unique keys
    val aggSalesDF = allSalesDF.sqlContext
      .sql("SELECT concat(datecalendaryear,'_',channel,'_',division,'_',gender,'_',category) as uniqueKey, " +
        "division, gender, category, channel,datecalendaryear as year, concat('W',weeknumberofseason) as week, " +
        "sum(netSales) as netSales, sum(salesUnits) as salesUnits " +
        "FROM sales "+
        "group by datecalendaryear, channel, division, gender, category, weeknumberofseason " +
        "order by uniqueKey")

    import org.apache.spark.sql.functions._

    //Modifying data to fit to Json output requirements
    val jsonResultDF = aggSalesDF.groupBy("uniqueKey")
      .agg(
        first("division").as("division"),
        first("gender").as("gender"),
        first("category").as("category"),
        first("channel").as("channel"),
        first("year").as("year"),
        first("week").as("weeknumberofseason"),
        first("netSales").as("netSales"),
        first("salesUnits").as("salesUnits"),
        collect_list(array("week", "netSales")).as("weeklyNetSales"),
        collect_list(array("week", "salesUnits")).as("weeklySalesUnits")
      )
      .select(col("uniqueKey").as("partitionByCol"),col("uniqueKey"),col("division"), col("gender"),
        col("category"), col("channel"), col("year"), col("weeklyNetSales"),col("weeklySalesUnits")
      )

    //Writing output to output directory
    jsonResultDF.write.partitionBy("partitionByCol").json(outputDir)
  }
}
