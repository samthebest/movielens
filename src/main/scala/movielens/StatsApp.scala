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
    val moviesPath: String =
      if (args.isDefinedAt(0)) args(0) else System.getProperty("user.dir") + "/data/ml-1m/movies.dat"

    val ratingsPath: String =
      if (args.isDefinedAt(1)) args(1) else System.getProperty("user.dir") + "/data/ml-1m/ratings.dat"

    val limit: Int = if (args.isDefinedAt(2)) args(2).toInt else 100

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
