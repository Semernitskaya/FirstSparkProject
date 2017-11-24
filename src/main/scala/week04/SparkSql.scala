import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql {

  val conf: SparkConf = new SparkConf().setAppName("week_04").setMaster("local[*]")

  val context: SparkContext = new SparkContext(conf)

  val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  val sqlContext = new SQLContext(context)

  import sqlContext.implicits._

  def getDataFrameFromTupleRdd = {
    val list = List((1, "ol"), (2, "vas"), (3, "vad"))
    val dataFrame = context.makeRDD(list).toDF();
    dataFrame.where("_2 = 'vas'").foreach(p => println(p))
  }

  def getDataFrameFromTupleRddWithStructure = {
    val list = List((1, "ol"), (2, "vas"), (3, "vad"))
    val dataFrame = context.makeRDD(list).toDF("id", "name");
    dataFrame.where("name = 'vas'").foreach(p => println(p))
  }

  def getDataFrameFromTupleRddWithCaseClass = {
    val list = List((1, "ol"), (2, "vas"), (3, "vad"))
    val dataFrame = context
      .makeRDD(list)
      .map(p => Person(p._1, p._2))
      .toDF();
    dataFrame.where("name = 'vas'").foreach(p => println(p))
  }

  def readDataFremeFromCsv = {
    session.read.csv(this.getClass.getResource("test.csv").toURI.getPath)
  }

  def readDataFremeFromJson = {
    session.read.json(this.getClass.getResource("test.json").toURI.getPath)
  }

  case class Person(id: Int, name: String)

  def main(args: Array[String]) {
    getDataFrameFromTupleRdd
    getDataFrameFromTupleRddWithStructure
    getDataFrameFromTupleRddWithCaseClass
  }

}