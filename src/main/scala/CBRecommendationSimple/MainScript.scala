package CBRecommendationSimple
import sys.process._
import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

object MainScript {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Movie Recommendation")
    conf.setMaster("local[2]")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "8g")
      .set("spark.executor.heartbeatInterval", "60")
      .set("spark.network.timeout", "600")
      .set("spark.driver.allowMultipleContexts", "true")

    val sparkContextObject = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContextObject)
    // automatically delete outputs from a previous runtime :)
    println("cleaning from previous running")
    var cleaningCmd = """rm -r out/ ;
                        rm -r out3/ ;
                        rm -r fuzzy/* """
    var proc0 = Seq("/bin/sh", "-c", cleaningCmd).!
      
    
    
    ContentBasedItemToItemSimilarity.itemSimilarities(sparkContextObject)
    var proc4 = Seq("/bin/sh", "-c","cat out3/itemtoitem/* >fuzzy/itemtoitem.csv ").!
    ContentBased.contentBased(sparkContextObject)
    var proc7 = Seq("/bin/sh", "-c","cat out3/predictionMatrix/* >fuzzy/usermoviesimi.csv").!
    ContentBasedJoinSimiTest.recommend(sparkContextObject)
    var proc5 = Seq("/bin/sh", "-c", "cat  out3/recommend4/* >fuzzy/recommendations.csv ").!
    ContentBasedPrecisionRecall.computePR(sparkContextObject, 5)
    ContentBasedPrecisionRecall.computePR(sparkContextObject, 10)
    ContentBasedDCG.DCG(sparkContextObject, 5)
    var proc6 = Seq("/bin/sh", "-c", "cat  out3/usersDCSG5/* >fuzzy/usersDCSG5 ").!
    ContentBasedIDCG.IDCG(sparkContextObject, 5)
    var proc8 = Seq("/bin/sh", "-c", "cat  out3/usersIDCSG5/* >fuzzy/usersIDCSG5 ").!
    ContentBasedNDCG.NDCG(sparkContextObject, 5)

    ContentBasedDCG.DCG(sparkContextObject, 10)
    var proc9 = Seq("/bin/sh", "-c", "cat  out3/usersDCSG10/* >fuzzy/usersDCSG10 ").!
    ContentBasedIDCG.IDCG(sparkContextObject, 10)
    var proc10 = Seq("/bin/sh", "-c", "cat  out3/usersIDCSG10/* >fuzzy/usersIDCSG10 ").!
    ContentBasedNDCG.NDCG(sparkContextObject, 10)

    ContentBasedDCG.DCG(sparkContextObject, 15)
    var proc11 = Seq("/bin/sh", "-c", "cat  out3/usersDCSG15/* >fuzzy/usersDCSG15 ").!
    ContentBasedIDCG.IDCG(sparkContextObject, 15)
    var proc12 = Seq("/bin/sh", "-c", "cat  out3/usersIDCSG15/* >fuzzy/usersIDCSG15 ").!
    ContentBasedNDCG.NDCG(sparkContextObject, 15)
    
     MAE.MAE(sparkContextObject)
  }

}