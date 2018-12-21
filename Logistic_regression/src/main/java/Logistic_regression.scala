import org.apache.spark.ml.classification.LogisticRegression
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


    val labeledTransformer = new StringIndexer().setInputCol("diabet").setOutputCol("label")
    val labeledFeatures = labeledTransformer.fit(features).transform(features)
    labeledFeatures.show(100)


    // Split data into training (60%) and test (40%)

    val splits = labeledFeatures.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0)
    val testData = splits(1)

    val lr = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.3)
      .setElasticNetParam(0.5)

    //Train Model
    val model = lr.fit(trainingData)

    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    //Make predictions on test data
    val predictions = model.transform(testData)

    //Evaluate the precision and recall
    val countProve = predictions.where("label == prediction").count()
    val count = predictions.count()

    println(s"Count of true predictions: $countProve Total Count: $count")
  }
}
