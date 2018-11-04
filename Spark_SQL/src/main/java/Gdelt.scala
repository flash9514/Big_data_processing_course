import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Gdelt {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    val dataSetSchema = StructType(Array(
      StructField("ID", IntegerType, true),
      StructField("YYYYMMDD", StringType, true),
      StructField("YYYYMM", StringType, true),
      StructField("YYYY", StringType, true),
      StructField("YYYY.smth", StringType, true),
      StructField("WhereShortly", StringType, true),
      StructField("Where", StringType, true),
      StructField("Country", StringType, true),
      StructField("Stuff1", StringType, true),
      StructField("Stuff2", StringType, true),
      StructField("Stuff3", StringType, true),
      StructField("Stuff4", StringType, true),
      StructField("Stuff5", StringType, true),
      StructField("Stuff6", StringType, true),
      StructField("Stuff7", StringType, true),
      StructField("Who2", StringType, true),
      StructField("Organizer", StringType, true),
      StructField("OrganizerShortly", StringType, true),
      StructField("Stuff21", StringType, true),
      StructField("Stuff22", StringType, true),
      StructField("Stuff23", StringType, true),
      StructField("Stuff24", StringType, true),
      StructField("Stuff25", StringType, true),
      StructField("Stuff26", StringType, true),
      StructField("Stuff27", StringType, true),
      StructField("Num1", StringType, true),
      StructField("Num2", StringType, true),
      StructField("Num3", StringType, true),
      StructField("Num4", StringType, true),
      StructField("Num5", StringType, true),
      StructField("Num6", StringType, true),
      StructField("Num7", StringType, true),
      StructField("Num8", StringType, true),
      StructField("Num9", StringType, true),
      StructField("Num10", StringType, true),
      StructField("Num11", StringType, true),
      StructField("Who3", StringType, true),
      StructField("Organizer3", StringType, true),
      StructField("OrganizerShortly3", StringType, true),
      StructField("Stuff31", StringType, true),
      StructField("Stuff32", StringType, true),
      StructField("Stuff33", StringType, true),
      StructField("Stuff34", StringType, true),
      StructField("Place", StringType, true),
      StructField("Stuff41", StringType, true),
      StructField("Stuff42", StringType, true),
      StructField("Stuff43", StringType, true),
      StructField("Stuff44", StringType, true),
      StructField("Stuff45", StringType, true),
      StructField("Stuff46", StringType, true),
      StructField("Place2", StringType, true),
      StructField("Stuff51", StringType, true),
      StructField("Stuff52", StringType, true),
      StructField("Stuff53", StringType, true),
      StructField("Stuff54", StringType, true),
      StructField("Stuff55", StringType, true),
      StructField("Stuff56", StringType, true),
      StructField("Link", StringType, true)))

    val events = sparkSession.read
      .option("header", "true")
      .option("delimiter", "\t")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .schema(dataSetSchema)
      .csv(args(0))

    events.show(100)

    println("Events took place in USA or RF:")
    events.select("ID","YYYYMMDD", "WhereShortly", "Where", "Place", "Place2", "Link").where("WhereShortly='RUS' OR WhereShortly='USA'").show()

    events.createOrReplaceTempView("eventTable")

    println("10 most mentioned persons (actors):")
    sparkSession.sql("SELECT Where as actor, COUNT(Where) as number FROM eventTable WHERE WhereShortly='CVL' GROUP BY Where ORDER BY number DESC LIMIT 10").show()


    val descSetSchema = StructType(Array(
      StructField("CAMEOcode", StringType, true),
      StructField("EventDescription", StringType, true)))

    val descriptions = sparkSession.read
      .option("header", "true")
      .option("delimiter", "\t")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .schema(descSetSchema)
      .csv(args(1))

    // join two DataFrames in order to map events to their corresponding descriptions
    val fullEvents = events.join(descriptions, events.col("Num2") === descriptions.col("CAMEOcode"))

    println("events with description:")
    fullEvents.show()

    fullEvents.createOrReplaceTempView("fullEventsTable")

    println("10 most mentioned events with description:")
    sparkSession.sql("SELECT COUNT(CAMEOcode) as number, CAMEOcode, EventDescription FROM fullEventsTable GROUP BY CAMEOcode, EventDescription ORDER BY number DESC LIMIT 10").show()
  }

}
