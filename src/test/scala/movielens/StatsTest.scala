package movielens

import org.specs2.mutable.Specification
import StaticSparkContext._

object StatsTest extends Specification {
  val ratings = sc.makeRDD(Seq(
    Rating(userID = 1, movieID = 1, rating = 5),
    Rating(userID = 1, movieID = 2, rating = 3),
    Rating(userID = 1, movieID = 3, rating = 4),
    Rating(userID = 3, movieID = 1, rating = 5),
    Rating(userID = 4, movieID = 1, rating = 5),
    Rating(userID = 4, movieID = 4, rating = 2),
    Rating(userID = 2, movieID = 3, rating = 5),
    Rating(userID = 2, movieID = 2, rating = 5),
    Rating(userID = 2, movieID = 1, rating = 5)
  ))

//  Rating(userID = 1, movieID = 2, rating = 3),
//  Rating(userID = 2, movieID = 2, rating = 5),

//  (5 + 3) / 2 = 4.0

  val movies = sc.makeRDD(Seq(
    Movie(1, "1t", List("a", "b")),
    Movie(2, "2t", List("a", "b", "c")),
    Movie(3, "3t", List("b")),
    Movie(4, "4t", List("a")),
    Movie(5, "5t", List("a")),
    Movie(6, "6t", List("d", "e"))
  ))

  "Stats.ratingsToUserStats" should {
    "Take an RDD of Ratings and return an RDD of UserStats, i.e. a list of users with no of movies they rated " +
      "and average rating per user" in {
      Stats.ratingsToUserStats(ratings).collect().toList.sortBy(_.userID) must_=== List(
        UserStats(userID = 1, numberOfMovies = 3, averageRating = 4.0),
        UserStats(userID = 2, numberOfMovies = 3, averageRating = 5.0),
        UserStats(userID = 3, numberOfMovies = 1, averageRating = 5.0),
        UserStats(userID = 4, numberOfMovies = 2, averageRating = 3.5)
      )
    }
  }

  "Stats.moviesToGenreCounts" should {
    "Take an RDD of Movies and return an RDD of GenreCounts, i.e. list of unique Genres and no of movies " +
      "under each genre" in {
      Stats.moviesToGenreCounts(movies).collect().toList.sortBy(_.genre) must_=== List(
        GenreCount("a", 4),
        GenreCount("b", 3),
        GenreCount("c", 1),
        GenreCount("d", 1),
        GenreCount("e", 1)
      )
    }
  }

  "Stats.topMovies" should {
    "Take an RDD of Movies and an RDD of Rating and return a RDD of MovieRank where the RDD is sorted by the rank" +
      " (1 being highest). Also only return N results" in {
      Stats.topMovies(movies, ratings, 3).collect().toList must_=== List(
        MovieRank(rank = 1, movieID = 1, title = "1t", averageRating = 5.0),
        MovieRank(rank = 2, movieID = 3, title = "3t", averageRating = 4.5),
        MovieRank(rank = 3, movieID = 2, title = "2t", averageRating = 4.0)
      )
    }
  }
}
