import org.apache.spark.{SparkConf, SparkContext}

object WordsCounter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("words-rdd")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    val inputFile = args(0)

    val fileRDD = sc.textFile(inputFile)

    val words = fileRDD.flatMap(s => s.split(" ")).map(s => (s, 1))

    val distinct = words.reduceByKey((accum, n) => (accum + n))

    distinct.foreach(println)
  }
}
