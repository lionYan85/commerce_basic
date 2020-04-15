import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, StructType, IntegerType, StringType}
import org.apache.spark.sql.{Row, SparkSession}

object RowNumber {

  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("adver0407").setMaster("local[4]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val array = Array("1,hadoop,12",
      "5,spark,16",
      "8,storm,18",
      "8,flume,20",
      "8,hive,10",
      "8,sparkstream,26",
      "10,tensorflow,26")

    val rdd = sparkSession.sparkContext.parallelize(array).map { row =>
      val Array(id, name, age) = row.split(",")
      Row(id, name, age.toInt)
    }

    val structType = new StructType(Array(
      StructField("id", StringType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    val  df = sparkSession.createDataFrame(rdd,structType)
    df.show()

    df.createOrReplaceTempView("technology")

    val result_1 = sparkSession.sql("select id,name,age from (select id,name,age,"+
    "row_number() over (partition by id order by age desc) top from technology) t where t.top <= 1")
    result_1.show()

  }

}
