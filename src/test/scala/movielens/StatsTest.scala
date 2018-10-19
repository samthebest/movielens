package movielens

import org.specs2.mutable.Specification
import StaticSparkContext._

object StatsTest extends Specification {
  "Stats.ratingsToUserStats" should {
    "Take an RDD of Ratings and return an RDD of UserStats, i.e. a list of users with no of movies they rated " +
      "and average rating per user" in {
      Stats.ratingsToUserStats(sc.makeRDD(Seq(
        Rating(userID = 1, movieID = 1, rating = 5),
        Rating(userID = 1, movieID = 2, rating = 3),
        Rating(userID = 1, movieID = 3, rating = 4),
        Rating(userID = 3, movieID = 1, rating = 5),
        Rating(userID = 4, movieID = 1, rating = 5),
        Rating(userID = 4, movieID = 4, rating = 4),
        Rating(userID = 2, movieID = 3, rating = 5),
        Rating(userID = 2, movieID = 2, rating = 5),
        Rating(userID = 2, movieID = 1, rating = 5)
      ))).collect().toList.sortBy(_.userID) must_=== List(
        UserStats(userID = 1, numberOfMovies = 3, averageRating = 4.0),
        UserStats(userID = 2, numberOfMovies = 3, averageRating = 5.0),
        UserStats(userID = 3, numberOfMovies = 1, averageRating = 5.0),
        UserStats(userID = 4, numberOfMovies = 2, averageRating = 4.5)
      )
    }
  }
}
