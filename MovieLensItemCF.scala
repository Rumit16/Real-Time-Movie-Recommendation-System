package org.uncc.moviereco

import java.util.Calendar

import scala.Ordering
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * @author Varda Laud
 * Item based Collaborative Filtering implementation in Spark Scala interface
 *
 */
object MovieLensItemCF {
  var simType: String = _
  var numberOfNeighbors: Int = _
  var numberOfReco: Int = _
  var ipFile: String = _
  var opFile: String = _
  var ipUri: String = _
  var opUri: String = _
  var topItemSimilarities: Array[(Int, Array[(Int, Double)])] = _

  /**
   * Finds similarity between all item pairs by using a similarity measure on the common ratings given by users to the items
   * @param itemPair (item1,item2)
   * @param userRatingPairs [(ratingByUserX,ratingByUserY)] <- Common user ratings for itemPair
   * @param simType (euclidean,cosine,pearson)
   * @return ((item1,item2),similarity)
   */
  def calculateSimilarity(itemPair: (Int, Int), userRatingPairs: Iterable[(Double, Double)], simType: String): ((Int, Int), Double) = {
    var sumXX, sumYY, sumXY, sumX, sumY, sumOfSquares = 0.0
    var n = 0
    for (userRatingPair <- userRatingPairs) {
      sumX += userRatingPair._1
      sumY += userRatingPair._2
      sumXX += Math.pow(userRatingPair._1, 2)
      sumYY += Math.pow(userRatingPair._2, 2)
      sumXY += userRatingPair._1 * userRatingPair._2
      sumOfSquares += Math.pow(userRatingPair._1
        - userRatingPair._2, 2);
      n += 1
    }
    var similarity = 0.0
    simType match {
      case "pearson" =>
        similarity = pearsonCorrelation(sumX, sumY, sumXX, sumYY, sumXY, n)
      case "cosine" =>
        similarity = cosine(sumXX, sumYY, sumXY)
      case "euclidean" =>
        similarity = euclidean(sumOfSquares)
    }
    return (itemPair, similarity)
  }

  /**
   * Calculates Pearson Correlation similarity
   * @param sumX
   * @param sumY
   * @param sumXX
   * @param sumYY
   * @param sumXY
   * @param n
   * @return
   */
  def pearsonCorrelation(sumX: Double, sumY: Double, sumXX: Double, sumYY: Double, sumXY: Double, n: Int): Double = {
    val numerator = sumXY - (sumX * sumY / n)
    val denominator = math.sqrt((sumXX - (Math.pow(sumX, 2) / n)) * (sumYY - (Math.pow(sumY, 2) / n)))
    if (denominator != 0) {
      return (numerator / denominator)
    }
    return 0.0
  }

  /**
   * Calculates Cosine similarity
   * @param sumXX
   * @param sumYY
   * @param sumXY
   * @return
   */
  def cosine(sumXX: Double, sumYY: Double, sumXY: Double): Double = {
    val numerator = sumXY
    val denominator = math.sqrt(sumXX) * math.sqrt(sumYY)
    if (denominator != 0) {
      return (numerator / denominator)
    }
    return 0.0
  }

  /**
   * Calculates similarity using euclidean distance
   * @param sumOfSquares
   * @return
   */
  def euclidean(sumOfSquares: Double): Double = {
    return (1 / (1 + sumOfSquares))
  }

  /**
   * Convert from ((item1,item2),similarity) to (item1,(item2,similarity)) and (item2,(item1,similarity))
   * @param itemPair (item1,item2)
   * @param similarity
   * @return
   */
  def keyOnFirstItem(itemPair: (Int, Int), similarity: Double): List[(Int, (Int, Double))] = {
    return List((itemPair._1, (itemPair._2, similarity)), (itemPair._2, (itemPair._1, similarity)))
  }

  /**
   * For every item finds its N most similar items
   * @param item
   * @param otherItemSimilarities
   * @param n
   * @return
   */
  def nearestNeighbors(item: Int, otherItemSimilarities: Array[(Int, Double)], n: Int): (Int, Array[(Int, Double)]) = {
    return (item, otherItemSimilarities.sortBy(c => c._2)(Ordering[Double].reverse).take(n))
  }

  /**
   * For a user finds top N recommended items
   * @param userId
   * @param itemRatings
   * @param topItemSimilarities
   * @param n
   * @return
   */
  def topNRecommendations(userId: Int, itemRatings: Map[Int, Double], topItemSimilarities: Map[Int, Array[(Int, Double)]], n: Int): ListBuffer[(Int, Int, Double)] = {
    val scores = HashMap.empty[Int, Double]
    val totalSim = HashMap.empty[Int, Double]
    for (similarity <- topItemSimilarities) {
      scores put (similarity._1, 0.0)
      totalSim put (similarity._1, 0.0)
    }
    // For every non rated item aggregate similary*rating and similarity 
    for (itemRating <- itemRatings) {
      val similarities = topItemSimilarities(itemRating._1)
      for (similarity <- similarities) {
        if (!itemRatings.contains(similarity._1)) {
          scores(similarity._1) += (similarity._2 * itemRating._2)
          totalSim(similarity._1) += (similarity._2)
        }
      }
    }
    // Normalize and take top N predictions
    val predictions = ListBuffer.empty[(Int, Int, Double)]
    val rankings = topItemSimilarities.toArray.map(x => (x._1, scores(x._1) / totalSim(x._1))).filter(x => !x._2.isNaN())
    for (ranking <- rankings.sortBy(c => c._2)(Ordering[Double].reverse).take(n)) {
      predictions.append((userId, ranking._1, ranking._2))
    }
    return predictions
  }

  /**
   * Calls other methods
   * @param userBasedRatings
   * @return
   */
  def getRecommendations(userBasedRatings: RDD[(Int, (Int, Double))]): RDD[(Int, Int, Double)] = {
    val groupedUserBasedRatings = userBasedRatings.groupByKey().map(x => (x._1, x._2.toArray.sortBy(c => c._1)))
    val itemPairsWithCommonUserRatings = groupedUserBasedRatings.flatMap(x => x._2.toArray.combinations(2).toArray.map(x => ((x(0)._1, x(1)._1), (x(0)._2, x(1)._2)))).groupByKey()
    val itemSimilarities = itemPairsWithCommonUserRatings.map(p => calculateSimilarity(p._1, p._2, simType)).flatMap(p => keyOnFirstItem(p._1, p._2)).groupByKey()
    topItemSimilarities = itemSimilarities.map(p => nearestNeighbors(p._1, p._2.toArray, numberOfNeighbors)).collect()
    val recommendations = groupedUserBasedRatings.flatMap(x => topNRecommendations(x._1, x._2.toMap, topItemSimilarities.toMap, numberOfReco))
    return recommendations;
  }

  /**
   * Reads and writes back to file system. Input is considered to be tab separated userId itemId rating
   */
  def fileReco(): RDD[(Int, Int, Double)] = {
	val conf = new SparkConf()
    .setMaster("local")
    .setAppName("MovieLensALS")
    .set("spark.executor.memory", "8g")
    .set("spark.hadoop.validateOutputSpecs", "false")
	val sc = new SparkContext(conf)
	val userBasedRatings = sc.textFile(ipFile).map { line =>
      val fields = line.split("\t")
      (fields(0).toInt, (fields(1).toInt, fields(2).toDouble))
    }
    val recommendations = getRecommendations(userBasedRatings)
    recommendations.saveAsTextFile(opFile)
    return recommendations;
  }

  /**
   * @param args
   * args[0] simType (euclidean,cosine,pearson)
   * args[1] numberOfNeighbors
   * args[2] numberOfReco
   * args[3] ipUri
   * args[4] opUri
   */
  def main(args: Array[String]) {
    simType = args(0)
    numberOfNeighbors = args(1).toInt
    numberOfReco = args(2).toInt
    ipFile = args(3)
    opFile = args(4)

    fileReco()
  }
}