package movielens

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoder, SparkSession}

case class Rating(userID: Int, movieID: Int, rating: Int)

// Genre is being handled as string, would be more efficient to use a `sealed` family of `case object`
case class Movie(movieID: Int, title: String, genres: List[String])

case class UserStats(userID: Int, numberOfMovies: Int, averageRating: Double)

case class GenreCount(genre: String, numberOfMovies: Long)

case class MovieRank(rank: Int, movieID: Int, title: String, averageRating: Double)

// Note we cannot use sparkSession.read for reading since it doesn't support delimeter.length > 1, nor nested lists
object IO {
  val inputDelimeter = "::"

  def readMovies(path: String)(implicit sc: SparkContext): RDD[Movie] =
    sc.textFile(path).map(_.split(inputDelimeter, -1).toList).map {
      case movieId :: title :: genres :: Nil => Movie(
        movieID = movieId.toInt,
        title = title,
        genres = genres.split("\\|").toList
      )
    }

  def readRatings(path: String)(implicit sc: SparkContext): RDD[Rating] =
    sc.textFile(path).map(_.split(inputDelimeter, -1).toList).map {
      case userID :: movieID :: rating :: _ :: Nil => Rating(
        userID = userID.toInt,
        movieID = movieID.toInt,
        rating = rating.toInt
      )
    }

  implicit class PimpedRDD[T: Encoder](data: RDD[T]) {
    def writeCSV(path: String)(implicit ss: SparkSession): Unit = {
      import ss.implicits._
      data.toDS().write.option("header", "false").csv(path)
    }
  }
}
