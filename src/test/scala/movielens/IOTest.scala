package movielens

import java.nio.file.{Files, Path}
import movielens.StaticSparkContext._
import org.apache.commons.io.FileUtils
import org.specs2.mutable.Specification
import IO._

object IOTest extends Specification {
  "IO.readMovies" should {
    "Read movies-sample.dat file and return RDD containing movie case classes corresponding to the file" in {
      readMovies(path = System.getProperty("user.dir") + "/src/test/resources/movies-sample.dat")
      .collect().toList.sortBy(_.movieID) must_===
        List(
          Movie(1, "Toy Story (1995)", List("Animation", "Children's", "Comedy")),
          Movie(2, "Jumanji (1995)", List("Adventure", "Children's", "Fantasy")),
          Movie(3, "Grumpier Old Men (1995)", List("Comedy", "Romance")),
          Movie(4, "Waiting to Exhale (1995)", List("Comedy", "Drama")),
          Movie(5, "Father of the Bride Part II (1995)", List("Comedy")),
          Movie(6, "Heat (1995)", List("Action", "Crime", "Thriller")),
          Movie(1287, "Sabrina (1995)", List("Comedy", "Romance")),
          Movie(2355, "Tom and Huck (1995)", List("Adventure", "Children's")),
          Movie(2804, "Sudden Death (1995)", List("Action"))
        )
    }
  }

  "IO.readRatings" should {
    "Read ratings-sample.dat file and return RDD containing ratings case classes corresponding to the file" in {
      readRatings(path = System.getProperty("user.dir") + "/src/test/resources/ratings-sample.dat")
      .collect().toList.sortBy(_.movieID) must_===
        List(
          Rating(1, 1193, 5),
          Rating(1, 661, 3),
          Rating(2, 914, 3),
          Rating(2, 3408, 4),
          Rating(1, 2355, 5),
          Rating(1, 1197, 3),
          Rating(1, 1287, 5),
          Rating(4, 2804, 5),
          Rating(4, 594, 3),
          Rating(4, 919, 4)
        ).sortBy(_.movieID)
    }
  }

  "IO.write" should {
    "Write some user stats to a file in CSV" in {
      val tempFile: Path = Files.createTempDirectory("MovieLensTest")

      val path = tempFile.toString + "/userStats"

      import ss.implicits._
      sc.makeRDD(Seq(
        UserStats(1, 1, 1.1),
        UserStats(2, 5, 1.1),
        UserStats(3, 10, 4.5)
      ))
      .writeCSV(path)

      val output = sc.textFile(path).collect().sorted.mkString("\n")

      FileUtils.deleteDirectory(tempFile.toFile)

      output must_===
        """1,1,1.1
          |2,5,1.1
          |3,10,4.5""".stripMargin
    }
  }
}
