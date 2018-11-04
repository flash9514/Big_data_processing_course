import org.apache.spark.sql.SparkSession

object Tweets {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    val df = sparkSession.read.json(args(0))

    println("Json schema")
    println(df.printSchema())

    df.createOrReplaceTempView("tweetTable")

    println("Tweet messages:")
    sparkSession.sql("SELECT object.body FROM tweetTable WHERE object.body IS NOT NULL").show(100)

    println("Count the most used languages:")
    sparkSession.sql("SELECT COUNT(object.actor.languages) AS number, object.actor.languages AS language FROM tweetTable WHERE object.actor.languages IS NOT NULL GROUP BY object.actor.languages ORDER BY number DESC").show(3)

    println("The earliest tweet date:")
    sparkSession.sql("SELECT MIN(postedTime) FROM tweetTable WHERE postedTime IS NOT NULL").show()

    println("The latest tweet date:")
    sparkSession.sql("SELECT MAX(postedTime) FROM tweetTable WHERE postedTime IS NOT NULL").show()

    println("The most used devices:")
    sparkSession.sql("SELECT COUNT(object.generator.displayName) AS number, object.generator.displayName AS device FROM tweetTable WHERE object.generator.displayName IS NOT NULL GROUP BY object.generator.displayName ORDER BY number DESC").show(3)

    println("All the tweets by user Sienna Cerros:")
    sparkSession.sql("SELECT object.body as messages FROM tweetTable WHERE object.body IS NOT NULL AND actor.displayName IS NOT NULL AND actor.displayName='Sienna Cerros'").show()

    println("How many twitters each user has:")
    sparkSession.sql("SELECT actor.displayName AS user, COUNT(actor.displayName) AS number FROM tweetTable WHERE actor.displayName IS NOT NULL GROUP BY actor.displayName").show()

    println("All persons mentioned on tweets:")
    df.select("object.body").where("object.body IS NOT NULL AND object.body LIKE '%@%'").show()

    println("All hashtags mentioned on tweets:")
    df.select("object.body").where("object.body IS NOT NULL AND object.body LIKE '%#%'").show()
  }

}
