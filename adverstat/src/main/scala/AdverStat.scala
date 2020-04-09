
import java.sql.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object AdverStat {

  def generateBlackList(adRealTimeFilterDStream: DStream[String]) = {
    {
      val key2NumDStream = adRealTimeFilterDStream.map {
        case log =>
          val logSplit = log.split(" ")
          val timeStamp = logSplit(0).toLong
          val dateKey = DateUtils.formatDate(new Date(timeStamp))
          val userId = logSplit(3).toLong
          val adid = logSplit(4).toLong

          val key = dateKey + "_" + userId + "_" + adid

          (key, 1L)
      }

      val key2CountDStream = key2NumDStream.reduceByKey(_ + _)

      //根据每一个RDD的数据更新用户点击次数表
      key2CountDStream.foreachRDD {
        rdd =>
          rdd.foreachPartition {
            items =>
              val clickCountArray = new ArrayBuffer[AdUserClickCount]()

              for ((key, count) <- items) {
                val keySplit = key.split("_")
                val date = keySplit(0)
                val userId = keySplit(1).toLong
                val adid = keySplit(2).toLong

                clickCountArray += AdUserClickCount(date, userId, adid, count)
              }

              AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
          }
      }


      val key2BlackListDStream = key2CountDStream.filter {
        case (key, count) =>
          val keySplit = key.split("_")
          val date = keySplit(0)
          val userId = keySplit(1).toLong
          val adid = keySplit(2).toLong

          val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adid)

          if (clickCount > 100) {
            true
          } else {
            false
          }
      }

      val userIdDStream = key2BlackListDStream.map {
        case (key, count) => key.split("_")(1).toLong
      }.transform(rdd => rdd.distinct())

      userIdDStream.foreachRDD {
        rdd =>
          rdd.foreachPartition {
            items =>
              val userIdArray = new ArrayBuffer[AdBlacklist]()

              for (userId <- items) {
                userIdArray += AdBlacklist(userId)
              }

              AdBlacklistDAO.insertBatch(userIdArray.toArray)
          }
      }
    }
  }

  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {

    //DStream[RDD[key,1L]]
    val key2ProvinceCityDStream = adRealTimeFilterDStream.map {
      case log =>
        val logSplit = log.split(" ")
        val dateKey = logSplit(0).toLong
        val province = logSplit(1)
        val city = logSplit(2)
        val adid = logSplit(4)

        val key = dateKey + "_" + province + "_" + city + "_" + adid
        (key, 1L)
    }

    //
    val key2StateDStream = key2ProvinceCityDStream.updateStateByKey[Long] {
      (values: Seq[Long], state: Option[Long]) =>
        var newValue = 0L
        if (state.isDefined)
          newValue = state.get
        for (value <- values) {
          newValue += value
        }

        Some(newValue)
    }

    //循环结果，存入数据库
    key2StateDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val adStateArray = new ArrayBuffer[AdStat]()
            for ((key, count) <- items) {
              val keySplit = key.split("_")
              val date = keySplit(0)
              val province = keySplit(1)
              val city = keySplit(2)
              val adid = keySplit(3).toLong

              adStateArray += AdStat(date, province, city, adid, count)
            }
            AdStatDAO.updateBatch(adStateArray.toArray)

        }
    }

    key2StateDStream
  }

  def provinceTop3Adver(sparkSession: SparkSession,
                        key2ProvinceCityCountDStream: DStream[(String, Long)]) = {
    //key2ProvinceCityCountDStream:RDD(key,count)
    //key:date_province_city_adid

    val key2ProvinceCountDStream = key2ProvinceCityCountDStream.map {
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adid = keySplit(3)

        val newKey = date + "_" + province + "_" + adid

        (newKey, count)
    }
    val key2ProvinceAggrCountDStream = key2ProvinceCountDStream.reduceByKey(_ + _)

    val top3DStream = key2ProvinceAggrCountDStream.transform {
      rdd => //rdd:RDD[(key,count)]
        val basidDateRDD = rdd.map {
          case (key, count) =>
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(2).toLong
            (date, province, adid, count)
        }
        import sparkSession.implicits._
        basidDateRDD.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_basic_info")

        val sql = "select date,province,adid,count from(" +
          "select date,province,adid,count, " +
          "row_number() over(partition by date,province order by count) rank from tmp_basic_info)t " +
          "where rank <= 3"

        sparkSession.sql(sql).rdd

    }

    top3DStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()
            for (item <- items) {
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adid = item.getAs[Long]("adid")
              val count = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date, province, adid, count)
            }

            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }

  }

  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]) = {
    val key2TimeMinuteDStream = adRealTimeFilterDStream.map {
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        val timeMinute = DateUtils.formatTimeMinute(new Date(timeStamp))

        val adid = logSplit(4).toLong
        val key = timeMinute + "_" + adid

        (key, 1L)
    }

    val key2WindowDStream = key2TimeMinuteDStream.reduceByKeyAndWindow((a: Long, b: Long) => a + b, Minutes(60), Minutes
    (1))

    key2TimeMinuteDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val trendArray = new ArrayBuffer[AdClickTrend]()
            for ((key, count) <- items) {
              val keySplit = key.split("_")
              val timeMinute = keySplit(0)
              val date = timeMinute.substring(0, 8)
              val hour = timeMinute.substring(8, 10)
              val minute = timeMinute.substring(10)
              val adid = keySplit(1).toLong

              trendArray += AdClickTrend(date, hour, minute, adid, count)
            }

            AdClickTrendDAO.updateBatch(trendArray.toArray)
        }
    }
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("adver0407").setMaster("local[4]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkContext = sparkSession.sparkContext

    //StreamingContext.getActiveOrCreate()
    val streamingContext = new StreamingContext(sparkContext, Duration(2000))

    //    // kafka消费者配置
    //    val kafkaParam = Map(
    //      "bootstrap.servers" -> kafka_brokers, //用于初始化链接到集群的地址
    //      "key.deserializer" -> classOf[StringDeserializer],
    //      "value.deserializer" -> classOf[StringDeserializer],
    //      //用于标识这个消费者属于哪个消费团体
    //      "group.id" -> "group1",
    //      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
    //      //latest：先去zookeeper获取offset，如果有，直接使用，如果没有，重最新的数据开始消费
    //      //earlist：先去zookeeper获取offset，如果有，直接使用，如果没有，重最开始的数据开始消费
    //      //none：先去zookeeper获取offset，如果有，直接使用，如果没有，直接报错
    //      "auto.offset.reset" -> "latest",
    //      //如果是true，则这个消费者的偏移量会在后台自动提交
    //      "enable.auto.commit" -> (false: java.lang.Boolean)
    //    )
    //
    //
    //    val adRealTimeDStream = KafkaUtils.createDirectStream(streamingContext,
    //      LocationStrategies.PreferConsistent,
    //      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam))

    //指定消费的 topic 名字
    val topic = "AdRealTimeLog0407"
    //指定kafka的broker地址(sparkStream的Task直连到kafka的分区上，用更加底层的API消费，效率更高)
    val brokerList = "172.16.12.10:9092,172.16.12.11:9092,172.16.12.12:9092"
    //创建 stream 时使用的 topic 名字集合，SparkStreaming可同时消费多个topic
    val topics: Set[String] = Set(topic)
    //准备kafka的参数
    val kafkaParams = mutable.HashMap[String, String]()
    kafkaParams.put("bootstrap.servers", brokerList)
    //必须添加以下参数，否则会报错
    kafkaParams.put("group.id", "group2")
    kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")


    //    var kafkaStream: InputDStream[(String, String)] = null
    //在Kafka中记录读取偏移量
    val adRealTimeDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      //位置策略
      PreferConsistent,
      //订阅的策略
      Subscribe[String, String](topics, kafkaParams)
    )

    val adRealTimeValueDStream = adRealTimeDStream.map(item => item.value())

    //取出Dstream里面每一条数据的value
    val adRealTimeFilterDStream = adRealTimeValueDStream.transform {
      logRDD =>

        val blackListArray = AdBlacklistDAO.findAll()
        val userIdArray = blackListArray.map(item => item.userid)

        //过滤logRDD里面的黑名单用户
        logRDD.filter {
          // log :timestamp province city userid aid

          case log =>
            val logSplit = log.split(" ")
            val userId = logSplit(3).toLong
            !userIdArray.contains(userId)

        }
    }


    streamingContext.checkpoint("./spark-streaming")

    //必须为streamingContext 时间的倍数
    adRealTimeFilterDStream.checkpoint(Duration(10000))
    //adRealTimeFilterDStream.foreachRDD(rdd => rdd.foreach(println(_)))
    //需求一  实时维护黑名单
    generateBlackList(adRealTimeFilterDStream)
    //需求二  各省各市一天中广告点击量
    provinceCityClickStat(adRealTimeFilterDStream)

    //需求三  统计各省热门广告
    val key2ProvinceCityCountDStream = provinceCityClickStat(adRealTimeFilterDStream)
    provinceTop3Adver(sparkSession, key2ProvinceCityCountDStream)

    //需求四  最近一小时广告点击量
    getRecentHourClickCount(adRealTimeFilterDStream)


    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
