package CBRecommendationSimple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import scala.collection.Iterator
import scala.collection.mutable.ListBuffer

object ContentBasedNDCG {
  val nb_genres = 18
  val simi_threshold = 0
  def parseMatrix(line: String): (Long, Long, Double) = {

    val ratingsRecord = line.split(",")

    val userId = ratingsRecord(0).toLong
    val movieId = ratingsRecord(1).toLong
    val ratings = ratingsRecord(2).toDouble

    (userId, movieId, ratings)

  }

  def NDCG(sparkContextObject:SparkContext, K:Int) {

     val t1 = System.currentTimeMillis() / 1000
    val userId = 14
    val similarityThreshold = 0.5

   val sqlContext = new SQLContext(sparkContextObject)

 
      
      val usersIDCSG = sparkContextObject.textFile("fuzzy/usersIDCSG"+K.toString())
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => {
        val record = line.split(";")
        val user = record(0).toLong
        val IDCG = record(1).toDouble
        
        (user, IDCG)
      }) 
      
       val usersDCSG = sparkContextObject.textFile("fuzzy/usersDCSG"+K.toString())
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => {
        val record = line.split(";")
        val user = record(0).toLong
        val IDCG = record(1).toDouble
        
        (user, IDCG)
      })
      

    //Calculate NDCG
    val usersDCGIDCG = usersDCSG.map(melem => (melem._1, melem)).join(usersIDCSG.map(nelem => (nelem._1, nelem)))
      .map({ case (k, (melem, nelem)) => (k, melem._2, nelem._2) }) //(user, DCG, IDCG)

    //usersDCGIDCG.saveAsTextFile("out3/DCGIDCG")
    val usersNDCG = usersDCGIDCG.map(t => (t._1, t._2/t._3))
   // usersNDCG.saveAsTextFile("out3/NDCG")
    //Avg of DCG and IDCG and NDCG
    val NDCG = usersNDCG.map(tuple => tuple._2).filter(!_.isNaN()).sum() / usersNDCG.map(tuple => tuple._2).filter(!_.isNaN()).count()

    println(usersNDCG.map(tuple => tuple._2).count())
    
    println("NDCG@"+ K +": "+ NDCG)
  }
  }
