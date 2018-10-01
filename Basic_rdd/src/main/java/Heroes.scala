import org.apache.spark.{SparkConf, SparkContext}

object Heroes {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("numbers-rdd-example")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    val inputFile = args(0)
    val fileRDD = sc.textFile(inputFile)

    val rowsRDD = fileRDD.map(s => s.split(";"))

    // number of each superhero
    val heroesCount = rowsRDD.map(line => (line.headOption.get, 1)).reduceByKey(_ + _)
    heroesCount.foreach(println)

    // number of enemies killed by each hero
    val enemiesCount = rowsRDD.map(line => (line.lift(1).get, line.lift(2).get.toInt)).reduceByKey(_ + _)
    enemiesCount.foreach(println)
  }
}
