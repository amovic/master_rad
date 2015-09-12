import magellan.{Point, Polygon}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

/**
 * @author cloudera
 */

object Test {
  def main(args: Array[String]){
 val conf = new SparkConf().setAppName("Point")
 val sc = new SparkContext(conf)
 val sqlContext = new SQLContext(sc)

 import sqlContext.implicits._

 val points = sc.parallelize(Seq((-1.0, -1.0), (-1.0, 1.0), (1.0, -1.0))).toDF("x", "y").select(point($"x", $"y").as("point"))

points.show() 
}}