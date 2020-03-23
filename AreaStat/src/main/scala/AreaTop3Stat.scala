import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}


object AreaTop3Stat {

  def getCityAndProductInfo(sparkSession: SparkSession, taskParmas: JSONObject) = {

    val startDate = ParamUtils.getParam(taskParmas, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParmas, Constants.PARAM_END_DATE)

    val sql = "select city_id,click_product_id from user_visit_action where date >= '" + startDate + "' and date <= " +
      "'" + endDate + "'and click_product_id != -1"

    import sparkSession.implicits._
    sparkSession.sql(sql).as[CityClickProduct].rdd.map {
      case cityPid => (cityPid.city_id, cityPid.click_product_id)
    }

  }

  def getCityAreaInfo(sparkSession: SparkSession): Unit = {

    val cityAreaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))

    sparkSession.sparkContext.makeRDD(cityAreaInfoArray).map {
      case (cityId, cityName, area) => (cityId, cityName, area)
    }

  }

  def getAreaPidBasicInfoTable(sparkSession: SparkSession
                               , cityId2PidRDD: RDD[(Long, Long)]
                               , cityId2AreaInfoRDD: RDD[(Long, CityAreaInfo)]): Unit = {
    val areaPidInfoRDD = cityId2PidRDD.join(cityId2AreaInfoRDD).map {
      case (cityId, (pid, areaInfo)) =>
        (cityId, areaInfo.city_name, areaInfo.area, pid)
    }

    import sparkSession.implicits._
    areaPidInfoRDD.toDF("city_id", "city_name", "area", "pid").createOrReplaceTempView("tmp_area_basic_info")
  }

  def getAreaProductClickCount(sparkSession: SparkSession): Unit = {

    val sql = "select area,pid,count(*) click_count," +
      "group_concat_distinct(concat_long_string(city_id,city_name,';'))city_infos" +
      " from tmp_area_basic_info group by area,pid"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_click_count")

  }


  def getAreaProductClickCountInfo(sparkSession: SparkSession): Unit = {

    val sql = "select tacc.area, tacc.city_infos,tacc.pid,pi.product_name," + "if(get_json_field(pi.extend_info," +
      "'product_status')='0','self','Third Party')product_status" +
      "tacc.click_count" +
      "from tmp_area_click_count tacc join product_info pi on tacc.pid = pi" +
      ".product_id"

    sparkSession.sql(sql).createOrReplaceTempView("tmp_area_count_info")
  }

  def getCityAndProductInfo(sparkSession: SparkSession, taskParmas: JSONObject) = ???

  def getTop3Product(sparkSession: SparkSession, taskUUID: String) = {
    val sqlData = "select area,city_infos,pid,product_name,product_status,click_count," + "row_number() over" +
      "(PARTITION BY " +
      "area order by click_count desc) rank from tmp_area_count_product_info"

    //    sparkSession.sql(sql).createOrReplaceTempView("temp_test")

    val sql = "select area," +
      "case" +
      "when area = '华北' or area = '华东' then 'A_Level'" +
      "when area = '华中' or area = '华南' then 'B_Level'" +
      "when area = '西南' or area = '西北' then 'C_Level'" +
      "else 'D_Level'" +
      "end Area_Level" +
      "city_infos,pid,product_name,product_stauts,click_count from (" +
      sqlData +
      ") t where rank <=3"

    val top3ProductRDD = sparkSession.sql(sql).rdd.map {
      case row =>
        AreaTop3Product(taskUUID,
          row.getAs[String]("area"),
          row.getAs[String]("area_Level"),
          row.getAs[Long]("pid"),
          row.getAs[String]("city_infos"),
          row.getAs[Long]("click_count"),
          row.getAs[String]("product_name"),
          row.getAs[String]("product_status"))
    }

    import sparkSession.implicits._
    top3ProductRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "area_top3_product_info")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
  }


  def main(args: Array[String]): Unit = {

    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParmas = JSONObject.fromObject(jsonStr)

    val taskUUID = UUID.randomUUID.toString

    val sparkConf = new SparkConf().setAppName("area").setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val cityId2PidRDD = getCityAndProductInfo(sparkSession, taskParmas)

    val cityId2AreaInfoRDD = getCityAreaInfo(sparkSession)

    getAreaPidBasicInfoTable(sparkSession, cityId2PidRDD, cityId2AreaInfoRDD)

    sparkSession.udf.register("concat_long_string", (v1: Long, v2: String, split: String) => {
      v1 + split + v2
    })

    sparkSession.udf.register("group_concat_distinct", new GrouoConcatDistinct)


    getAreaProductClickCount(sparkSession)

    sparkSession.udf.register("get_json_field", (json: String, field: String) => {
      val jSONObject = JSONObject.fromObject(jSONObject)
      jSONObject.getString(field)
    })


    getAreaProductClickCountInfo(sparkSession)

    getTop3Product(sparkSession, taskUUID)

    sparkSession.sql("select * from tmp_area_basic_info").show()

  }

}
