package movielens

import org.specs2.mutable.Specification

object StatsAppTest extends Specification {
  "StatsApp.main when given input files movies-sample.dat and ratings-sample.dat" should {
    StatsApp.main(Array(
      System.getProperty("user.dir") + "/src/test/resources/movies-sample.dat",
      System.getProperty("user.dir") + "/src/test/resources/ratings-sample.dat"
    ))

    "Output a directory called user-stats in ./data/target " +
      "containing CSV files which contains a list of users with no of movies they rated " +
      "and average rating per user"

    "Output a directory called genre-counts in ./data/target " +
      "containing CSV files which contains a list of unique Genres and no of movies " +
      "under each genre"

    "Output a directory called top-movies in ./data/target " +
      "containing parquet files which contains the top 100 movies based on their ratings. " +
      "This should have fields, Rank (1-100), Movie Id, Title, Average Rating. Rank 1 is the most popular movie."
  }
}
