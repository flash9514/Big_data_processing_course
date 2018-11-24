import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object Logistic_regression {

  def main(args: Array[String]): Unit = {

    val inputFile = args(0)

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-read-csv")
      .master("local[*]")
      .getOrCreate()


    //Read CSV file to DF and define scheme on the fly
    val patients = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(inputFile)


    /*patients.show(100)
    patients.printSchema()*/

    //Feature Extraction

    val DFAssembler = new VectorAssembler().

      setInputCols(Array(
        "pregnancy", "glucose", "arterial pressure",
        "thickness of TC", "insulin", "body mass index",
        "heredity", "age")).
      setOutputCol("features")

    val features = DFAssembler.transform(patients)
    features.show(100)


    /*val labeledTransformer = new StringIndexer().setInputCol("diabet").setOutputCol("label")
    val labeledFeatures = labeledTransformer.fit(features).transform(features)
    labeledFeatures.show(100)*/


    // Split data into training (60%) and test (40%)

    val splits = features.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)
  }
}
