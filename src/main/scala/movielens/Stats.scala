package movielens

import org.apache.spark.rdd.RDD

object Stats {
  def ratingsToUserStats(ratings: RDD[Rating]): RDD[UserStats] = {
    ???
  }
}
