package CBRecommendationSimple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

object ContentBased {
  val nb_genres = 18
  def parseMatrix(line: String): (Long, Long, Double) = {

    val ratingsRecord = line.split(",")

    val userId = ratingsRecord(0).toLong
    val movieId = ratingsRecord(1).toLong
    val ratings = ratingsRecord(2).toDouble

    (userId, movieId, ratings)

  }

  //scalar product of two vectors
  def scalarProduct(l1: List[Int], l2: List[Int]): Int = {
    val sp = for (i <- 0 to nb_genres - 1) yield l1(i) * l2(i)
    sp.sum
  }

  def transformGenres(genres: Set[String]): List[Int] = {
    val genresVector = new Array[Int](nb_genres)
    for (genre <- genres) {
      genre match {
        case "(unknown)" => None
        case "Action" => genresVector(0) = 1
        case "Adventure" => genresVector(1) = 1
        case "Animation" => genresVector(2) = 1
        case "Children" => genresVector(3) = 1
        case "Comedy" => genresVector(4) = 1
        case "Crime" => genresVector(5) = 1
        case "Documentary" => genresVector(6) = 1
        case "Drama" => genresVector(7) = 1
        case "Fantasy" => genresVector(8) = 1
        case "Film-Noir" => genresVector(9) = 1
        case "Horror" => genresVector(10) = 1
        case "Musical" => genresVector(11) = 1
        case "Mystery" => genresVector(12) = 1
        case "Romance" => genresVector(13) = 1
        case "Sci-Fi" => genresVector(14) = 1
        case "Thriller" => genresVector(15) = 1
        case "War" => genresVector(16) = 1
        case "Western" => genresVector(17) = 1
        case "IMAX" => None

      }
    }
    genresVector.toList
  }

  def parseMovieFile(line: String): (Long, String, Set[String]) = {

    val moviesRecord = line.split(",").toList

    val movieId = moviesRecord(0).toLong

    val movieGenres = moviesRecord(moviesRecord.length - 1)
    val genresArray = movieGenres.split("\\|").toList.toSet[String].map(_.replaceAll("\"", ""))

    def removeFirstAndLast[A](xs: Iterable[A]) = xs.drop(1).dropRight(1)

    def parseMovieName(list: List[String]): String = {
      removeFirstAndLast(list).mkString(",").replaceAll("^\"", "").replaceAll("\"$", "")
    }

    val movieName = if (moviesRecord.length > 3) {
      parseMovieName(moviesRecord)
    } else {
      moviesRecord(1)
    }

    (movieId, movieName, genresArray)

  }
  //calculer une prediction pour chaque film dans le data set pour chaque user
  //un score est calculé: le score du film est la somme pondére de son degre de similarité au film mieux preféres multiplié par le ratings
  def generatePredictionMatrix(userMatrix: RDD[((Long, Long), Double)], itemToItemMatrix: RDD[((Long, Long), Double)]): RDD[((Long, Long), Double)] = {
    //pour chaque filme evalué chercher ses films similaires
    //joindre sur le film evalué
    val joinedMatrix = userMatrix.map(mElement => (mElement._1._2, mElement))
      .join(itemToItemMatrix.map(nElement => (nElement._1._1, nElement)))//.
      .map {
      //pour chaque film regarder obtenir un nouveau film  et le user et le score.
        case (k, (mElement, nElement)) => {
        //  (user, nouveaufilm), (ratings du film * similarité, rating)
         ((mElement._1._1, nElement._1._2), (mElement._2 * nElement._2, mElement._2))
        }
      }
   // println(joinedMatrix.count())
    val aggregateMatrix = joinedMatrix.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(tuple => tuple._1 / tuple._2)

    aggregateMatrix

  }

  def contentBased(sparkContextObject:SparkContext) {

    val t1 = System.currentTimeMillis() / 1000
    val userId = 14
    val similarityThreshold = 0.5

    val conf = new SparkConf().setAppName("Movie Recommendation")
    conf.setMaster("local[2]")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "8g")
      .set("spark.executor.heartbeatInterval", "60")
      .set("spark.network.timeout", "600")

   
    val sqlContext = new SQLContext(sparkContextObject)

    /*
     * movies description
     */
   

   
    //cartesion product: square of movie matrix
    val itemToItemMatrix = sparkContextObject.textFile("fuzzy/itemtoitem.csv")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line =>
        {
          val record = line.split(";")
          val m1 = record(0).toLong
          val m2 = record(1).toLong
          val simi = record(2).toDouble
          ((m1,m2), simi)
        })

    /*
     * rating matrix filtring
     */
   
      
   /* val rates = sparkContextObject
      .textFile("data/ratings.csv")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(parseMatrix)
      .map(tuple=> (tuple._1, tuple))
      .join(users.map(tuple=> (tuple,tuple)))
      .map({ case (k, (melem, nelem)) => (melem._1, melem._2, melem._3) })*/
      

    val trainingRatingRDD = sparkContextObject
      .textFile("data/train/ratings.csv")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(parseMatrix)
    /*  .map(t=> {
        val (u,m,r) = t
        var  rating: Double = 0
        if (r>=0.6)
          rating = 1
        (u,m, rating)  
        
      })*/
     // .map(melem => (melem._1,  melem))
     // .join(users.map(tuple=> (tuple,tuple)))
     // .map({ case (k, (melem, nelem)) => (melem._1, melem._2, (melem._3 + 1 )/2) })
    //  .cache()

  
   /* val testRatingRDD = sparkContextObject
      .textFile("data/test/ratings.csv")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(parseMatrix)
      .map(melem => (melem._1,  melem))
      .join(users.map(tuple=> (tuple,tuple)))
      .map({ case (k, (melem, nelem)) => (melem._1, melem._2, (melem._3+1 )/2) })
      .cache()*/
   
    val highestRating = trainingRatingRDD/*filter(tuple=> tuple._3>=3)*/
    .groupBy(tuple=> tuple._1)
    .map(t=>{
      val (u,itr) = t
      val mostrated = itr.toList.sortBy(tuple => tuple._3)(Ordering[Double].reverse).take(10)
      
      (u,itr)
    })
    .flatMap({
        case (key, groupValues) =>
          groupValues.map { value => ((value._1, value._2), value._3) }
      })
    val predictionMatrix: RDD[((Long, Long), Double)] = generatePredictionMatrix(highestRating, itemToItemMatrix)

    predictionMatrix.map(tuple=>  s"${tuple._1._1},${tuple._1._2},${tuple._2}").saveAsTextFile("out3/predictionMatrix")

   
   
  }

}