import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object NumbersProcessing {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setAppName("numbers-rdd")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    val inputFile = args(0)
    val fileRDD = sc.textFile(inputFile)

    val rowsRDD = fileRDD.map(s => s.split(" "))
    val numbers = rowsRDD.map(s => s.map(t => t.toInt))

    println("Sum of numbers of each row")
    val rowsSum = numbers.map(t => t.reduce(_ + _))
    rowsSum.foreach(println)

    println()
    println("Sum of numbers of each row, which are multiples of 5")
    val multiplesOfFive = numbers.map(s => s.filter(_ % 5 == 0))
    multiplesOfFive.map(t => if(t.isEmpty) 0 else t.reduce(_+_)).foreach(println)

    println()
    println("Max numbers of each row")
    numbers.map(t => t.max).foreach(println)

    println()
    println("Min numbers of each row")
    numbers.map(t => t.min).foreach(println)

    println()
    println("Set of distinct numbers in each row")
    numbers.map(t => t.distinct.foreach(println)).foreach(println)

    val flatRows = fileRDD.flatMap(s => s.split(" "))
    val flatNumbers = flatRows.map(s => s.toInt)

    println()
    println("Sum of all numbers")
    println(flatNumbers.reduce((acc, a) => acc + a))

    println()
    println("Sum of numbers, which are multiples of 5")
    println(flatNumbers.filter(_ % 5 == 0).reduce(_+_))

    println()
    println("Max number")
    println(flatNumbers.max)

    println()
    println("Min number")
    println(flatNumbers.min)

    println()
    println("Set of distinct numbers")
    flatNumbers.distinct.foreach(println)
  }
}
