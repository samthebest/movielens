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

  "Stats.moviesToGenreCounts" should {
    "Take an RDD of Movies and return an RDD of GenreCounts, i.e. list of unique Genres and no of movies " +
      "under each genre" in {
      Stats.moviesToGenreCounts(sc.makeRDD(Seq(
        Movie(1, "", List("a", "b")),
        Movie(2, "", List("a", "b", "c")),
        Movie(3, "", List("b")),
        Movie(4, "", List("a")),
        Movie(5, "", List("a")),
        Movie(5, "", List("d", "e"))
      ))).collect().toList.sortBy(_.genre) must_=== List(
        GenreCount("a", 4),
        GenreCount("b", 3),
        GenreCount("c", 1),
        GenreCount("d", 1),
        GenreCount("e", 1)
      )
    }
  }
}
