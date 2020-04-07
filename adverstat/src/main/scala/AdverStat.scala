
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import commons.conf.ConfigurationManager
import commons.constant.Constants
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


object AdverStat {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()


    //StreamingContext.getActiveOrCreate()
    val steamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))
    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)


    // kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers, //用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "group1",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      //latest：先去zookeeper获取offset，如果有，直接使用，如果没有，重最新的数据开始消费
      //earlist：先去zookeeper获取offset，如果有，直接使用，如果没有，重最开始的数据开始消费
      //none：先去zookeeper获取offset，如果有，直接使用，如果没有，直接报错
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val adRealTimeDStream = KafkaUtils.createDirectStream(steamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam))

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
    adRealTimeFilterDStream

  }


}
