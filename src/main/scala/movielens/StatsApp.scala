package movielens

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

// TODO Use Scallop for getopts parsing
object StatsApp {
  val userStatsPath = System.getProperty("user.dir") + "/data/target/user-stats"
  val genreCountsPath = System.getProperty("user.dir") + "/data/target/genre-counts"
  val topMoviesPath = System.getProperty("user.dir") + "/data/target/top-100-movies"

  def main(args: Array[String]): Unit = {
    val moviesPath: String =
      if (args.isDefinedAt(0)) args(0) else System.getProperty("user.dir") + "/src/test/resources/movies.dat"

    val ratingsPath: String =
      if (args.isDefinedAt(1)) args(1) else System.getProperty("user.dir") + "/src/test/resources/ratings.dat"

    val n: Int = if (args.isDefinedAt(2)) args(2).toInt else 100

    val sparkConf: SparkConf = new SparkConf().setAppName("MovieLens").setMaster("local")
    implicit val ss: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val sc: SparkContext = ss.sparkContext

    val movies: RDD[Movie] = IO.readMovies(moviesPath)
    val ratings: RDD[Rating] = IO.readRatings(ratingsPath)

    import ss.implicits._

    IO.write(userStatsPath, Stats.ratingsToUserStats(ratings))
    IO.write(genreCountsPath, Stats.moviesToGenreCounts(movies))
  }
}
