
import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object SessionStates {

  def main(args: Array[String]): Unit = {

    val jsonPar = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    val taskParams = JSONObject.fromObject(jsonPar)

    val taskUUID = UUID.randomUUID.toString

    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val actionRDD = getOrIActionRDD(sparkSession, taskParams)

    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id, item))

    val session2GroupActionRDD = sessionId2ActionRDD.groupByKey()

    session2GroupActionRDD.cache()

    session2GroupActionRDD.foreach(println(_))

    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, session2GroupActionRDD)

    val sessionAccumulator = new SessionAccumulator
    sparkSession.sparkContext.register(sessionAccumulator)

    //    sessionId2FullInfoRDD.foreach(println(_))
    val sessionID2FilterRDD = getSessionFilterRDD(taskParams, sessionId2FullInfoRDD, sessionAccumulator)

    sessionID2FilterRDD.foreach(println(_))

    getSessionRatio(sparkSession,taskUUID,sessionAccumulator.value)

  }

  def getSessionRatio(session: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]):Unit ={

    val session_count = value.getOrElse(Constants.SESSION_COUNT,1).toDouble

    val visitLength1_3 =  value.getOrElse(Constants.TIME_PERIOD_1s_3s,0)//依次到180s

    val stepLength1_3 =  value.getOrElse(Constants.STEP_PERIOD_1_3,0)//依次到180s

    val visitLength1_3ratio = NumberUtils.formatDouble(visitLength1_3/session_count , 2)//依次到180s

  }

  def getOrIActionRDD(sparkSession: SparkSession, taskParams: JSONObject) = {

    val startDate = ParamUtils.getParam(taskParams, Constants.PARAM_START_DATE)

    val endDate = ParamUtils.getParam(taskParams, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "' "

    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd

  }

  def getSessionFilterRDD(taskParam: JSONObject
                          , sessionId2FullInfoRDD: RDD[(String, String)]
                          , sessionAccumulator: SessionAccumulator) = {

    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)

    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)

    val professional = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val city = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIDs = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professional != null) Constants.PARAM_PROFESSIONALS + "=" + professional + "|" else "") +
      (if (city != null) Constants.PARAM_CITIES + "=" + city + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIDs != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIDs + "|" else "")

    if (filterInfo.endsWith("\\|")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    }

    sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) => {
        var success = true

        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants
          .PARAM_END_AGE)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        } else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        } else if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }

        if (success) {
          sessionAccumulator.add(Constants.SESSION_COUNT)

          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong

          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong

          if (visitLength >= 1 && stepLength <= 3) {
            sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
          }else if(visitLength >= 4 && stepLength <= 6){
            sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
          }

          calculateVisitLength(visitLength,sessionAccumulator)

          calculateStepLength(stepLength,sessionAccumulator)

        }

        success
      }

      //        if(success)

    }
  }


  def calculateVisitLength(visitLength:Long , sessionAccumulator: SessionAccumulator) = {

    if(visitLength >= 1 && visitLength <=3){
      sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    }else if(visitLength >= 4 && visitLength <=6){
      sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    }//一直到180秒

  }

  def calculateStepLength(stepLength:Long , sessionAccumulator: SessionAccumulator) = {

    if(stepLength >= 1 && stepLength <=3){
      sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    }else if(stepLength >= 4 && stepLength <=6){
      sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    }//一直到180秒

  }

  def getSessionFullInfo(sparkSession: SparkSession
                         , session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {

    val userId2AggrInfoRDD = session2GroupActionRDD.map {

      case (sessionId, iterableAction) =>
        var userId = -1L

        var startTime: Date = null
        var endTime: Date = null

        var stepLength = 0
        val searchKeywords = new StringBuffer(" ")
        val clickCategories = new StringBuffer(" ")

        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }

          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }

          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }

          val searchKeyword = action.search_keyword
          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeywords + ",")

          }

          val clickCateID = action.click_category_id
          if (clickCateID != -1 && !clickCategories.toString.contains(clickCateID)) {
            clickCategories.append(clickCateID + ",")
          }

          stepLength += 1

        }


        val searchKW = StringUtils.trimComma(searchKeywords.toString)
        val clickcg = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickcg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggrInfo)
    }

    val sql = "select * from user_info"

    val userId2InfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (
      item.user_id, item
    ))

    val sessionId2FullInfoRDD = userId2AggrInfoRDD.join(userId2InfoRDD).map {
      case (userId, (aggrInfo, userInfo)) =>

        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val fullInfo = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionID = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionID, fullInfo)
    }
    sessionId2FullInfoRDD
  }


}
