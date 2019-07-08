package CBRecommendationSimple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import scala.collection.Iterator
import scala.collection.mutable.ListBuffer

object ContentBasedPrecisionRecall {
  val nb_genres = 18
  val simi_threshold = 0
  def parseMatrix(line: String): (Long, Long, Double) = {

    val ratingsRecord = line.split(",")

    val userId = ratingsRecord(0).toLong
    val movieId = ratingsRecord(1).toLong
    val ratings = ratingsRecord(2).toDouble

    (userId, movieId, ratings)

  }

 



  def computePR(sparkContextObject:SparkContext,N:Int) {

   
    val conf = new SparkConf().setAppName("Movie Recommendation")
    conf.setMaster("local[2]")
      .set("spark.executor.memory", "6g")
      .set("spark.driver.memory", "8g")
      .set("spark.executor.heartbeatInterval", "60")
      .set("spark.network.timeout", "600")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "16g")

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
      .map(tuple => ((tuple._1, tuple._2), (tuple._3) )) //normalize from [-1, 1] to [0,1]
     
    val join = sparkContextObject.textFile("fuzzy/recommendations.csv")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => {
        val record = line.split(";")
        val user = record(0).toLong
        val movie = record(1).toLong
        val asimilarity = record(2).toDouble
        val presimi = record(3).toDouble
        (user, movie, asimilarity, presimi)
      })
   val prperUser = join.groupBy(tuple => tuple._1)
      .flatMap(t => {
        val (u, itr) = t
        var tp: Int = 0
        var fp: Int = 0
        var tn: Int = 0
        var fn: Int = 0
        val list = itr.toList.sortBy(tuple => tuple._3)(Ordering[Double].reverse)
         val aratingSum = list.map(rate => rate._3).sum
        val pratingSum = list.map(rate => rate._4).sum
        val avgActual =  aratingSum/list.size
        val avgPredi =  pratingSum/list.size
        for (i <- 0 to math.min(N - 1, list.size-1)) {
          val tuple = list(i)
          if (tuple._3 < avgActual && tuple._4 > avgPredi)
            fp = fp + 1
          if (tuple._3 > avgActual && tuple._4 > avgPredi)
            tp = tp + 1
          if (tuple._3 > avgActual && tuple._4 < avgPredi)
            fn = fn + 1
          if (tuple._3 < avgActual && tuple._4 < avgPredi)
            tn = tn + 1
        }
        if (tp + fp != 0 && tp + fn != 0)
          Some(u, tp.toDouble / (tp + fp), tp.toDouble / (tp + fn))
        else
          None

      })
 //   prperUser.saveAsTextFile("out3/prperUser")
    val precision = prperUser.map(tuple => tuple._2).sum / prperUser.count()
    println("precision  @ "+N +": :" + precision)

    val recall = prperUser.map(tuple => tuple._3).sum / prperUser.count()
    println("recall :  @ "+N +": :" + recall)
    
    val f1 = (2* precision * recall ) / (precision+ recall)
    println("f1 :  @ "+N +": :" +f1)
  }
  }
