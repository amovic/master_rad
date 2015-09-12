import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.magellan.MagellanContext
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import magellan._

/**
 * @author cloudera
 */
object TestShape {
 def main(args: Array[String]){
 val conf = new SparkConf().setAppName("Brojevi_parcela")
 val sc = new SparkContext(conf)
  val sqlCtx = new MagellanContext(sc)
    
    
    import sqlCtx.implicits._
    var path = "parc"
    val dfParc = sqlCtx.read.format("magellan").option("type", "geojson").load(path)
    
    path = "brparc"
    val dfBrParc = sqlCtx.read.format("magellan").option("type", "geojson").load(path)
    
    //dfBrParc.join(dfParc).where(dfBrParc.col("point") intersects dfParc.col("polygon")).show()
    
    var parcBrParc = dfBrParc.select($"point", $"metadata"("TxtMemo")).join(dfParc.select($"polygon"))
      .where(dfBrParc.col("point") intersects dfParc.col("polygon"))
      
    /*
     * { "type": "Feature", "properties": { "Id": null }, "geometry": { "type": "Polygon", "coordinates": [ [ [ 6558038.34, 4698581.04 ], [ 6558039.7, 4698579.6 ], [ 6558039.56, 4698577.62 ], [ 6558038.68, 4698565.11 ], [ 6558036.71, 4698565.15 ], [ 6558030.05, 4698565.29 ], [ 6558025.59, 4698565.39 ], [ 6558023.75, 4698565.43 ], [ 6558023.54, 4698581.3 ], [ 6558038.34, 4698581.04 ] ] ] } }
     */
    
    var polygonJson = "{\"type\": \"FeatureCollection\",\"features\": [ ";
    
    parcBrParc.collect.foreach(row => {
      polygonJson += "{ \"type\": \"Feature\", \"properties\": { \"Id\": \"%s\" },".format(row.getString(1))
      polygonJson += "\"geometry\": { \"type\": \"Polygon\", \"coordinates\": [ [ " 
      val polygon = row.getAs[Polygon]("polygon")
      polygon.points.foreach { point => {
          polygonJson += "[ %f, %f ],".format(point.x, point.y)
        } 
      }
      polygonJson = polygonJson.substring(0, polygonJson.length() - 1)
      polygonJson += " ] ] } \n},";
      
    })
    polygonJson = polygonJson.substring(0, polygonJson.length() - 1)
    polygonJson += "  ] }";
    println(polygonJson)
    
}
}