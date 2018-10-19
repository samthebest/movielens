package movielens

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StaticSparkContext {
  val sparkConf = new SparkConf().setAppName("MovieLens").setMaster("local")

  implicit val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  implicit val sc = ss.sparkContext
}
