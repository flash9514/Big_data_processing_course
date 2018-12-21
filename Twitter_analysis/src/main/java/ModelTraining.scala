import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.sql.SparkSession

object ModelTraining {

  //Featurize Function
  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }

  def main(args: Array[String]): Unit = {

    val jsonFile = "C:\\Users\\zufar\\Documents\\Everything\\Big_data_processing_course\\Twitter_analysis\\tweets\\"
    val modelOutput = "C:\\Users\\zufar\\Documents\\Everything\\Big_data_processing_course\\Twitter_analysis\\model\\"

    System.setProperty("hadoop.home.dir", "C:\\HADOOP");

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-tweets-train-kmeans")
      .master("local[*]")
      .getOrCreate()

    //Read json file to DF
    val tweetsDF = sparkSession.read.json(jsonFile)

    //Show the first 100 rows
    tweetsDF.show(100);

    //Extract the text
    val text = tweetsDF.select("text").rdd.map(r => r(0).toString)

    //Get the features vector
    val features = text.map(s => featurize(s))

    val numClusters = 10
    val numIterations = 40

    // Train KMenas model and save it to file
    val model: KMeansModel = KMeans.train(features, numClusters, numIterations)
    model.save(sparkSession.sparkContext, modelOutput)
  }

}
