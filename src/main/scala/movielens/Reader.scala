package movielens

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class Rating(userID: Int, movieID: Int, rating: Int)

case class Movie(movieID: Int, title: String, genres: List[String])

object Reader {
  def readMovies(path: String)(implicit sc: SparkContext): RDD[Movie] =
    sc.textFile(path).map(_.split("::", -1).toList).map {
      case movieId :: title :: genres :: Nil => Movie(
        movieID = movieId.toInt,
        title = title,
        genres = genres.split("\\|").toList
      )
    }
}
