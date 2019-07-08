package CBRecommendationSimple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import scala.collection.Iterator
import scala.collection.mutable.ListBuffer

object ContentBasedDCG {
  val nb_genres = 18
  val simi_threshold = 0
  def parseMatrix(line: String): (Long, Long, Double) = {

    val ratingsRecord = line.split(",")

    val userId = ratingsRecord(0).toLong
    val movieId = ratingsRecord(1).toLong
    val ratings = ratingsRecord(2).toDouble

    (userId, movieId, ratings)

  }

  def DCG(sparkContextObject:SparkContext,K:Int) {

    val t1 = System.currentTimeMillis() / 1000
    val userId = 14
    val similarityThreshold = 0.5

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
   
  
    val rates = sparkContextObject.textFile("data/test/ratings.csv") //(userId, movieId, rating)
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(parseMatrix)
      .map(tuple => ((tuple._1, tuple._2), (tuple._3))) //normalize from [-1, 1] to [0,1]
     
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
  

    //DCG computing
   
    val RankInPred = userMoviSimilarity.groupBy(tuple => tuple._1) // (user, movie, pRate, pRank)
      .map(t => {
        val (u, itr) = t
        val order = itr.toList.sortBy(tuple => tuple._2)(Ordering[Double].reverse).zipWithIndex.map(tuple => (tuple._1._1._1, tuple._1._1._2, tuple._1._2, tuple._2 + 1))
        val it = order.iterator
        val lb = new ListBuffer[(Long, Long, Double, Int)]
        var next = it.next()
        lb.append(next)

        var prev = next
        var indx = 1
        var execo = 0
        while (it.hasNext) {
          next = it.next()
          if (next._3 != prev._3) {
            indx = indx + 1
          }
          lb.append((next._1, next._2, next._3, indx))

          prev = next
        }

        (u, lb.toList)
      })
      .flatMap({
        case (key, groupValues) =>
          groupValues.map { value => (value._1, value._2, value._3, value._4) }
      }) // ajouter 1 car in commence de 0

    val RankInTest = rates.groupBy(tuple => tuple._1) // (user, movie, aRate, aRank)
      .map(t => {
        val (u, itr) = t
        val order = itr.toList.sortBy(tuple => tuple._2)(Ordering[Double].reverse).zipWithIndex.map(tuple => (tuple._1._1._1, tuple._1._1._2, tuple._1._2, tuple._2 + 1))
        val it = order.iterator
        val lb = new ListBuffer[(Long, Long, Double, Int)]
        var next = it.next()
        lb.append(next)

        var prev = next
        var indx = 1

        while (it.hasNext) {
          next = it.next()
          if (next._3 != prev._3) {
            indx = indx + 1
          }
          lb.append((next._1, next._2, next._3, indx))

          prev = next
        }

        (u, lb.toList)

      })
      .flatMap({
        case (key, groupValues) =>
          groupValues.map { value => (value._1, value._2, value._3, value._4) }
      }) // ajouter 1 car in commence de 0
    //RankInPred.saveAsTextFile("out3/rankinPred")
    // RankInTest.saveAsTextFile("out3/rankintest")

    //join RankIntest and rankinpred
    val joinPredAndtest = RankInPred.map(melem => ((melem._1, melem._2), melem)).join(RankInTest.map(nelem => ((nelem._1, nelem._2), nelem)))
      .map({ case (k, (melem, nelem)) => (k._1, k._2, melem._3, melem._4, nelem._3, nelem._4) }) //(userid, movieid, prate, prank, arate, arank)
   // joinPredAndtest.saveAsTextFile("out3/joinPredAndtest")
    //calculate DCG
    val usersDCSG = joinPredAndtest.groupBy(tuple => tuple._1)//group by user
      .map(t => { //userDCG = arate(0) + somme(1 to K) arate/log_2 (prank)
        val (u, itr) = t

        val order = itr.toList.sortBy(tuple => tuple._3)(Ordering[Double].reverse).take(K) //sort by pradicted rate
        val it = order.iterator
        val lb = new ListBuffer[(Long, Long, Double, Int, Double, Int)] //updates the ranks according to movies of test set
        var next = it.next()
        lb.append(next)
        var prev = next
        var indx = 1
        while (it.hasNext) {
          next = it.next()
          //    if (next._5 != prev._5) {
          indx = indx + 1
          //    }

          lb.append((next._1, next._2, next._3, indx, next._5, next._6))
          prev = next
        }
        // var userDCG: Double = 0
        var userDCG = lb(0)._5
        for (i <- 1 to order.length - 1) {
          //  userDCG = userDCG + ((Math.pow(2, lb(i)._5) - 1) / Math.log(lb(i)._4 + 1))
          userDCG = userDCG + (lb(i)._5 / Math.log(lb(i)._4))
        }
        (u, userDCG)

      })
  usersDCSG.map(tuple=>  s"${tuple._1};${tuple._2};${tuple._2}").saveAsTextFile("out3/usersDCSG"+K.toString())
    }
  
  }

