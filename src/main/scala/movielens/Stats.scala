package movielens

import org.apache.spark.rdd.RDD

case class CountAndAverageRating(movieCount: Int, ratingTotal: Int)

object Stats {
  def ratingsToUserStats(ratings: RDD[Rating]): RDD[UserStats] =
    ratings.map(rating => rating.userID -> CountAndAverageRating(1, rating.rating))
    .reduceByKey {
      case (left, right) =>
        CountAndAverageRating(left.movieCount + right.movieCount, left.ratingTotal + right.ratingTotal)
    }
    .map {
      case (userId, CountAndAverageRating(movieCount, ratingTotal)) =>
        UserStats(userId, movieCount, ratingTotal.toDouble / movieCount)
    }

  def moviesToGenreCounts(movies: RDD[Movie]): RDD[GenreCount] = {
    ???
  }
}
