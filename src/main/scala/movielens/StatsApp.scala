package movielens

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import IO._

// TODO Use Scallop for getopts parsing
object StatsApp {
  val userStatsPath = System.getProperty("user.dir") + "/data/target/user-stats"
  val genreCountsPath = System.getProperty("user.dir") + "/data/target/genre-counts"
  val topMoviesPath = System.getProperty("user.dir") + "/data/target/top-movies"

  def main(args: Array[String]): Unit = {
    val moviesPath: String = args(0)
    val ratingsPath: String = args(1)
    val limit: Int = args(2).toInt

    val sparkConf: SparkConf = new SparkConf().setAppName("MovieLens").setMaster("local")
    implicit val ss: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val sc: SparkContext = ss.sparkContext

    val movies: RDD[Movie] = readMovies(moviesPath)
    val ratings: RDD[Rating] = readRatings(ratingsPath)

    import ss.implicits._

    Stats.ratingsToUserStats(ratings).writeCSV(userStatsPath)
    Stats.moviesToGenreCounts(movies).writeCSV(genreCountsPath)
    Stats.topMovies(movies, ratings, limit).toDF().write.parquet(topMoviesPath)
  }
}
