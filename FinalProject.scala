package FinalProject

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object FinalProject {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]");
    val sc = new SparkContext(conf);

    val cities: Array[(Double, Double)] = processCities(sc)
    cities.foreach(println)
  }

  def processCities(sc: SparkContext): Array[(Double, Double)] = {
    val cities = sc.textFile("./src/main/scala/FinalProject/cities.txt")
    cities
      .map(x => x.split(","))
      .map({case Array(x, y) => (x.trim.toDouble.toString.take(5).toDouble, y.trim.toDouble.toString.take(5).toDouble)})
      .collect()
  }

  def getClusters(cities: Array[(Double, Double)]): Unit = {

  }

}