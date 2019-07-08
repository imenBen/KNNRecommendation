package CBRecommendationSimple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import scala.collection.Iterator
import scala.collection.mutable.ListBuffer

object MAE {
  val nb_genres = 18
  val simi_threshold = 0
  def parseMatrix(line: String): (Long, Long, Double) = {

    val ratingsRecord = line.split(",")

    val userId = ratingsRecord(0).toLong
    val movieId = ratingsRecord(1).toLong
    val ratings = ratingsRecord(2).toDouble

    (userId, movieId, ratings)

  }

  def MAE(sparkContextObject:SparkContext) {

    val t1 = System.currentTimeMillis() / 1000
    val userId = 14
    val similarityThreshold = 0.5

       val sqlContext = new SQLContext(sparkContextObject)

     val join = sparkContextObject.textFile("fuzzy/recommendations.csv")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => {
        val record = line.split(";")
        val user = record(0).toLong
        val movie = record(1).toLong
        val asimilarity = record(2).toDouble//normalize from [-1 1] to [0 1]
        val presimi = record(3).toDouble
        (user, movie, asimilarity, presimi)
      })
  
    val recommendationCount = join.map(tuple=> math.pow((tuple._3)-tuple._4, 2)).filter(!_.isNaN()).count()
    println("recommendationCount  :"+recommendationCount)
    val  RMSE_2 = join.map(tuple=> math.pow((tuple._3)-tuple._4, 2)).filter(!_.isNaN()).sum()/recommendationCount
   // println ("RMSE :"+ RMSE_2.sum())
    
    val RMSE = math.sqrt(RMSE_2)
    
    val MAE = join.map(tuple=> math.abs((tuple._3)-tuple._4)).filter(!_.isNaN()).sum()/recommendationCount
    
    println("MAE: "+ MAE)
    println("RMSE : "+ RMSE)
  // RMSE_2.saveAsTextFile("out3/RMSE")
     }
  
  }

