package movielens

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class Rating(userID: Int, movieID: Int, rating: Int)

case class Movie(movieID: Int, title: String, genres: List[String])

case class UserStats(userID: Int, numberOfMovies: Int, averageRating: Double)

// Note we cannot use sparkSession.read for reading since it doesn't support "::" delimeter, nor nested lists
object IO {
  val delimeter = "::"

  def readMovies(path: String)(implicit sc: SparkContext): RDD[Movie] =
    sc.textFile(path).map(_.split(delimeter, -1).toList).map {
      case movieId :: title :: genres :: Nil => Movie(
        movieID = movieId.toInt,
        title = title,
        genres = genres.split("\\|").toList
      )
    }

  def readRatings(path: String)(implicit sc: SparkContext): RDD[Rating] =
    sc.textFile(path).map(_.split(delimeter, -1).toList).map {
      case userID :: movieID :: rating :: _ :: Nil => Rating(
        userID = userID.toInt,
        movieID = movieID.toInt,
        rating = rating.toInt
      )
    }

  def writeUserStats(path: String, userStats: RDD[UserStats])(implicit ss: SparkSession): Unit = {
    import ss.implicits._
    userStats.toDS().write.option("header", "false").csv(path)
  }
}
