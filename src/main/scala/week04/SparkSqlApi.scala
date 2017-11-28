package week04

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//todo: move to test suite
object SparkSqlApi {

  val conf: SparkConf = new SparkConf().setAppName("week_04").setMaster("local[*]")

  val context: SparkContext = new SparkContext(conf)
  context.setLogLevel("WARN")

  val sqlContext = new SQLContext(context)

  import sqlContext.implicits._

  def getDataFrame: DataFrame = {
    val list = List((1, "ol", "28"),
      (2, "vas", "23"),
      (3, "vad", "30"))
    val dataFrame = context
      .makeRDD(list)
      .map(p => Person(p._1, p._2, p._3.toInt))
      .map(p => {
        if (p.name == "vas") p.hobby = "bicycle"
        p;
      })
      .toDF();
    return dataFrame
  }

  case class Person(id: Long,
                    name: String,
                    age: Int,
                    var hobby: String = null,
                    salary: Float = Float.NaN)

  def main(args: Array[String]) {
    //TODO: separate test cases
    //actions
    getDataFrame.show
    getDataFrame.printSchema

    //transformations
    //drop
    getDataFrame.na.drop.show
    getDataFrame.na.drop("all").show
    getDataFrame.na.drop(Array("id", "hobby")).show

    //fill
    getDataFrame.na.fill(0).show
    getDataFrame.na.fill("unknown hobby").show
    getDataFrame.na.fill(Map("salary" -> 99.99,
      "hobby" -> "unknown hobby")).show

    //replace
    getDataFrame.na.replace("id", Map(3 -> 33)).show

    //select
    getDataFrame.select("id", "hobby").show

    //ToDO: null check ???
    //filter|where
    getDataFrame.filter($"id" > 2).show
    getDataFrame.where("hobby != null").show
    getDataFrame.filter($"id" > 2 && $"hobby" == null).show
  }
}

