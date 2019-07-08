package CBRecommendationSimple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import scala.collection.Iterator
import scala.collection.mutable.ListBuffer

object RecommendationTests {
  val nb_genres = 18
  val simi_threshold = 0
  def parseMatrix(line: String): (Long, Long, Double) = {

    val ratingsRecord = line.split(",")

    val userId = ratingsRecord(0).toLong
    val movieId = ratingsRecord(1).toLong
    val ratings = ratingsRecord(2).toDouble

    (userId, movieId, ratings)

  }

 



  def main(args: Array[String]) {

    val t1 = System.currentTimeMillis() / 1000
    val userId = 14
    val similarityThreshold = 0.5

    val conf = new SparkConf().setAppName("Movie Recommendation")
    conf.setMaster("local[2]")
      .set("spark.executor.memory", "7g")
      .set("spark.driver.memory", "8g")
      .set("spark.executor.heartbeatInterval", "60")
      .set("spark.network.timeout", "600")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "16g")
      .set("spark.storage.memoryFraction", "0.8")

    val sparkContextObject = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContextObject)

    val Recommendations = sparkContextObject.textFile("fuzzy/recommendations.csv")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => {
        val record = line.split(";")
        val user = record(0).toLong
        val movie = record(1).toLong
        val rate = record(2).toDouble
        val simi = record(3).toDouble
        val score = record(4).toDouble
        val nb = record(5).toDouble
        ((user, movie), rate, simi,score , nb)
      })
      .sortBy(tuple => tuple._1._1)
      
       Recommendations.saveAsTextFile("out3/sortedRecommendations")
     }
  
 
  }
