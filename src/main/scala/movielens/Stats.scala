package movielens

import org.apache.spark.rdd.RDD

case class CountAndRatingTotal(movieCount: Int, ratingTotal: Int)

object Stats {
  def addCountAndRatingTotal(left: CountAndRatingTotal, right: CountAndRatingTotal): CountAndRatingTotal =
    CountAndRatingTotal(left.movieCount + right.movieCount, left.ratingTotal + right.ratingTotal)

  def ratingsToUserStats(ratings: RDD[Rating]): RDD[UserStats] =
    ratings.map(rating => rating.userID -> CountAndRatingTotal(1, rating.rating))
    .reduceByKey(addCountAndRatingTotal)
    .map {
      case (userId, CountAndRatingTotal(movieCount, ratingTotal)) =>
        UserStats(userId, movieCount, ratingTotal.toDouble / movieCount)
    }

  def moviesToGenreCounts(movies: RDD[Movie]): RDD[GenreCount] =
    movies.flatMap(_.genres).map((_, 1L)).reduceByKey(_ + _).map {
      case (genre, count) => GenreCount(genre, count)
    }

  // Observe we avoid a join here, but we assume `limit` is not very large. Considerably different algorithm required for
  // large `limit`
  def topMovies(movies: RDD[Movie], ratings: RDD[Rating], limit: Int): RDD[MovieRank] = {
    val ratingsMap: Map[Int, (Double, Int)] =
      ratings.map(rating => rating.movieID -> CountAndRatingTotal(1, rating.rating))
      .reduceByKey(addCountAndRatingTotal)
      .mapValues(countAndRatingTotal => countAndRatingTotal.ratingTotal.toDouble / countAndRatingTotal.movieCount)
      .top(limit)(Ordering.by(_.swap))
      .zipWithIndex.map {
        case ((movieID, averageRating), rank) => movieID ->(averageRating, rank + 1)
      }
      .toMap

    movies.flatMap(movie => ratingsMap.get(movie.movieID).map {
      case (averageRating, rank) => MovieRank(rank, movie.movieID, movie.title, averageRating)
    })
    .sortBy(_.rank)
  }
}
