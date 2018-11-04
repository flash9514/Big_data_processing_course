import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object MarketBasketAnalysis {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("market-basket-analysis")
      .master("local[*]")
      .getOrCreate()

    var dataSetSchema = StructType(Array(
      StructField("InvoiceNo", StringType, true),
      StructField("StockCode", StringType, true),
      StructField("Description", StringType, true),
      StructField("Quantity", IntegerType, true),
      StructField("InvoiceDate", StringType, true),
      StructField("UnitPrice", DoubleType, true),
      StructField("CustomerID", IntegerType, true),
      StructField("Country", StringType, true)))

    val gdelt = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .schema(dataSetSchema)
      .csv(args(0))

    gdelt.createOrReplaceTempView("dataSetTable")

    var dataFrameOfInvoicesAndStockCodes = gdelt.select("InvoiceNo", "StockCode").where("InvoiceNo IS NOT NULL AND StockCode IS NOT NULL")

    val keyValue = dataFrameOfInvoicesAndStockCodes.rdd.map(row => (row(0), row(1).toString))

    val groupedKeyValue = keyValue.groupByKey()
    val transactions = groupedKeyValue.map(row => row._2.toArray.distinct)


    //get frequent patterns via FPGrowth
    val fpg = new FPGrowth()
      .setMinSupport(0.02)

    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach { itemset =>
      println(s"${itemset.items.mkString("[", ",", "]")}, ${itemset.freq}")
    }

    //get association rules
    val minConfidence = 0.4
    val rules = model.generateAssociationRules(minConfidence)

    val dictionary = gdelt.select("StockCode", "Description")

    rules.collect().foreach { rule =>
      println(
        rule.antecedent.map(code => dictionary.select("Description").where("StockCode='" + code + "'").collectAsList().get(0).toString()).mkString("[", ",", "]")
          + " => " + rule.consequent.map(code => dictionary.select("Description").where("StockCode='" + code + "'").collectAsList().get(0).toString()).mkString("[", ",", "]")
          + ", " + rule.confidence)
    }

  }

}
