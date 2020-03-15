import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object AreaTop3Stat {

  def cgetCityAndProductInfo(sparkSession: SparkSession, taskParmas: JSONObject) = {

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

  def getCityAndProductInfo(sparkSession: SparkSession, taskParmas: JSONObject) = ???

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



    sparkSession.sql("select * from tmp_area_basic_info").show()

  }

}
