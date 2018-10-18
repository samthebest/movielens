package movielens

import org.specs2.mutable.Specification
import StaticSparkContext.sc

object ReaderTest extends Specification {
  "Reader.readMovies" should {
    "Read movies-sample.dat file and return RDD containing movie case classes corresponding to the file" in {
      Reader.readMovies(path = System.getProperty("user.dir") + "/src/test/resources/movies-sample.dat")
      .collect().toList.sortBy(_.movieID) must_===
        List(
          Movie(1, "Toy Story (1995)", List("Animation", "Children's", "Comedy")),
          Movie(2, "Jumanji (1995)", List("Adventure", "Children's", "Fantasy")),
          Movie(3, "Grumpier Old Men (1995)", List("Comedy", "Romance")),
          Movie(4, "Waiting to Exhale (1995)", List("Comedy", "Drama")),
          Movie(5, "Father of the Bride Part II (1995)", List("Comedy")),
          Movie(6, "Heat (1995)", List("Action", "Crime", "Thriller")),
          Movie(7, "Sabrina (1995)", List("Comedy", "Romance")),
          Movie(8, "Tom and Huck (1995)", List("Adventure", "Children's")),
          Movie(9, "Sudden Death (1995)", List("Action"))
        )
    }
  }
}
