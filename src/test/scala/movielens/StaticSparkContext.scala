package movielens

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object StaticSparkContext {
  val sparkConf: SparkConf = new SparkConf().setAppName("MovieLens").setMaster("local")
  implicit val ss: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  implicit val sc: SparkContext = ss.sparkContext
}
