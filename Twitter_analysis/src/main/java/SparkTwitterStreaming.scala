import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkTwitterStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("language-classifier")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(30))

    // Configure your Twitter credentials
    val apiKey = "KNVTwiYajvHab5NBqBWSWepde"
    val apiSecret = "62eWYe5bBZlf09mXzpdgtOAzSBm7FxlkoZCgCYmRblpTOSI4wu"
    val accessToken = "863078539347861504-eWeovitnbZ2g8eNinenoMTz1PTfMbOY"
    val accessTokenSecret = "UP1tsTaFuVXTmDJ3nIxGYo0mhqt3ybOuClEQ1V4oTWvbT"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    System.setProperty("hadoop.home.dir", "C:\\HADOOP\\");

    // Create Twitter Stream in JSON
    val tweets = TwitterUtils
      .createStream(ssc, None)
      .map(new Gson().toJson(_))


    val numTweetsCollect = 10000L
    var numTweetsCollected = 0L
    val path = "C:\\Users\\zufar\\Documents\\Everything\\Big_data_processing_course\\Twitter_analysis\\tweets\\"

    //Save tweets in file
    tweets.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.coalesce(1)
        outputRDD.saveAsTextFile(path + time)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsCollect) {
          System.exit(0)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
