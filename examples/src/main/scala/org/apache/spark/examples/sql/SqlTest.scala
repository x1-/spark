package org.apache.spark.examples.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SqlTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SqlTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    val df1 = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i"))).toDF()
    df1.registerTempTable("records")

    println("Result of SELECT *:")
    sqlContext.sql("SELECT * FROM records").collect().foreach(println)

    val df2 = sc
      .parallelize(0 to 1000)
      .map(i => Tuple2(i, i / 100.0))
      .toDF()

    println( df2.schema )
    sc.stop()
  }

}
