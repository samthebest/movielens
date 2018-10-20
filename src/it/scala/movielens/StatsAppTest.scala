package movielens

import java.io.File
import java.nio.file.{Files, Path}

import org.specs2.mutable.Specification
import StaticSparkContext._
import org.apache.commons.io.FileUtils
import StatsApp._

object StatsAppTest extends Specification {
  "StatsApp.main when given input files movies-sample.dat and ratings-sample.dat" should {
    main(Array(
      System.getProperty("user.dir") + "/src/test/resources/movies-sample.dat",
      System.getProperty("user.dir") + "/src/test/resources/ratings-sample.dat"
    ))

    import ss.implicits._

    val (userStats, genreCounts) =
      try {
        val userStats: String = sc.textFile(userStatsPath).collect().toList.sortBy(_.take(1)).mkString("\n")
        val genreCounts: String = sc.textFile(genreCountsPath).collect().toList.sortBy(_.take(2)).mkString("\n")
        //    val topMovies: String = ss.read.parquet(topMoviesPath).as[MovieRank].collect().toList.sortBy(_.movieID)

        (userStats, genreCounts)
      } finally {
        FileUtils.deleteDirectory(new File(userStatsPath))
        FileUtils.deleteDirectory(new File(genreCountsPath))
        FileUtils.deleteDirectory(new File(topMoviesPath))
      }


    "Output a directory called user-stats in ./data/target " +
      "containing CSV files which contains a list of users with no of movies they rated " +
      "and average rating per user" in {
      userStats must_===
        s"""1,5,${(5 + 3 + 5 + 3 + 5).toDouble / 5}
           |2,2,${(3 + 4).toDouble / 2}
           |4,3,${(5 + 4 + 3).toDouble / 3}""".stripMargin
    }

    "Output a directory called genre-counts in ./data/target " +
      "containing CSV files which contains a list of unique Genres and no of movies " +
      "under each genre" in {

      genreCounts must_===
        """Action,2
          |Adventure,2
          |Animation,1
          |Children's,3
          |Comedy,5
          |Crime,1
          |Drama,1
          |Fantasy,1
          |Romance,2
          |Thriller,1""".stripMargin
    }

    "Output a directory called top-100-movies in ./data/target " +
      "containing parquet files which contains the top 100 movies based on their ratings. " +
      "This should have fields, Rank (1-100), Movie Id, Title, Average Rating. Rank 1 is the most popular movie."
  }
}
