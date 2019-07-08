package CBRecommendationSimple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import scala.collection.Iterator
import scala.collection.mutable.ListBuffer

object ContentBasedJoinSimiTest {
  val nb_genres = 18
  val simi_threshold = 0
  def parseMatrix(line: String): (Long, Long, Double) = {

    val ratingsRecord = line.split(",")

    val userId = ratingsRecord(0).toLong
    val movieId = ratingsRecord(1).toLong
    val ratings = ratingsRecord(2).toDouble

    (userId, movieId, ratings)

  }

 



  def recommend(sparkContextObject:SparkContext) {

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

       // val sparkContextObject = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContextObject)

    val userMoviSimilarity = sparkContextObject.textFile("fuzzy/usermoviesimi.csv")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => {
        val record = line.split(",")
        val user = record(0).toLong
        val movie = record(1).toLong
        val similarity = record(2).toDouble
        
        ((user, movie), similarity)
      })
    /*
       *  test  : Precision and recall metrics
       */
  // looking for highly rated movies
    val rates = sparkContextObject.textFile("data/test/ratings.csv") //(userId, movieId, rating)
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(parseMatrix)
      .map(tuple => ((tuple._1, tuple._2), (tuple._3 ) )) //normalize from [-1, 1] to [0,1]
     
   // join with similarity matrix
    val join = rates.map(rElem => ((rElem._1._1, rElem._1._2), rElem)).join(userMoviSimilarity.map(sElem => ((sElem._1._1, sElem._1._2), sElem)) )
      .map({ case ((user, movies), (relem, selem)) => (user, movies, relem._2, selem._2) })
    //   .sortBy(tuple => tuple._1, true)
    join.map(tuple => s"${tuple._1};${tuple._2};${tuple._3};${tuple._4}").saveAsTextFile("out3/recommend4")
  }
  
  }
