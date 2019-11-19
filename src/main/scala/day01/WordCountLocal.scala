package day01

import org.apache.spark.{SparkConf, SparkContext}

object WordCountLocal {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("in")
    val sum = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    val result = sum.collect()
    println(result)

  }
}
