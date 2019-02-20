package com.spark.exactlyonce.streaming

import com.alibaba.fastjson.{JSON, JSONObject}
import com.spark.kafka.Producer.KafkaMsgBean
import com.spark.exactlyonce.zk.{ZkPartitionOffsetHandler, ZkPartitionOffsetParser}
import com.spark.exactlyonce.zk.{ZkPartitionOffsetHandler, ZkPartitionOffsetParser}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
  * Created by terry on 2018/10/1.
  */
object ExactlyKafkaSparkStreamingApp {

  val zkHandler = ZkPartitionOffsetHandler

  /**
    * User's implements this method as service logic
    *
    **/
  def serviceLogicOp(streamLines: DStream[String]) = {
    // here we do service level operations
    // 1. calculate the step by step either by SQLContext sqls or RDD directly transforms && actions
    // 2. save/sync results to external storages like redis/kafka/hdfs/hbase/es by different apis
    val resultRDD = streamLines.window(new Duration(60 * 1000)).map(item => {
      val jsonObj: JSONObject = JSON.parseObject(item, classOf[JSONObject])
      var bean: KafkaMsgBean = new KafkaMsgBean(jsonObj.getString("id"),
        jsonObj.getString("msg"), jsonObj.getString("timestamp"))
      // timestamp yyyy-mm-dd HH:MM:SS
      val key =  bean.timestamp
      // we only get the yyyy-mm-dd HH:MM:SS and take this as groupBy operation's key
      val value = bean
      (key, value)
    }).groupByKey.map(item => {
      val timestampValue: String = item._1
      val durationRecordCount: Int = item._2.size
      (timestampValue, durationRecordCount)
    })
    resultRDD
  }


  /**
    * load OffsetRange's from external storage
    * here we use the zookeeper as the ''external storage''
    *
    * @param topicPartitionNumMap in this map name of the topic will be used as the key ,
    *                             value is the partition number that how many partitions in total under this topic in kafka cluster
    * @return Map[TopicPartition,Long] in this map , TopicPartition can contain the info which topic ,
    *         and which partition and the Long can contain the partition's offset value = untilOffset - fromOffset
    **/
  def loadRangeOffsetFromExternal(topicPartitionNumMap: Map[String, Int]): Map[TopicPartition, Long] = {
    var topicPartition = Map[TopicPartition, Long]()
    for (topicName: String <- topicPartitionNumMap.keySet) {
      val partitionNum: Int = topicPartitionNumMap.get(topicName).get
      if (!zkHandler.isTopicCached(topicName)) {
        println(s"begin add path for topic =${topicName} with partitonNum=${partitionNum}")
        zkHandler.addTopicPartitionNum(topicName, partitionNum)
      }
      for (partitionId: Int <- 0 until partitionNum) {
        val offsetRange: OffsetRange = zkHandler.readRangeOffset(topicName, partitionId)
        topicPartition += (offsetRange.topicPartition() -> offsetRange.count())
        println(s"[topic]=${topicName},[partitionId]=${partitionId},[offset]=${offsetRange.count()}")
      }
    }
    topicPartition
  }

  /**
    * save the offsetRange back to external storage
    * here we use the zookeeper as our ''external storage''
    *
    * @param offsetRange in which contains the
    *                    1. topic name : String
    *                    2. partition id : Int
    *                    3. fromOffset value :Long
    *                    4. untilOffset value :Long
    *                    5. count: untilOffset - fromOffset
    **/
  def syncRangeOffsetToExternalStorage(offsetRange: OffsetRange) = {
    println(s"[syncRangeOffsetToExternalStorage]," +
      s"[OffsetRange]=${ZkPartitionOffsetParser.fromOffsetRange(offsetRange)}")
    zkHandler.updateRangeOffset(offsetRange)
  }

  /**
    * In this method we create direct stream between spark and kafka
    * and we also use the offsets storaged in ''external storage'' to init the stream
    *
    * @param ssc            StreamingContext
    * @param topicPartition key:topic name , value: how many partitions in total in this topic
    * @return DStream[String] the data stream from kafka to Spark StreamingContext
    **/
  def kafkaDStreamInit(ssc: StreamingContext, brokers: String, topicPartition: Map[String, Int],
                       loadCache: Boolean): InputDStream[ConsumerRecord[String, String]] = {
    val group = "spark-kafka-streaming-exactly-once"
    val topics = topicPartition.keySet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = loadCache match {
      case true =>
        KafkaUtils.createDirectStream[String, String](
          ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams,
            loadRangeOffsetFromExternal(topicPartition))
        )
      case false =>
        KafkaUtils.createDirectStream[String, String](
          ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)
        )
    }
    stream
  }


  /**
    * In this method we commit the consumed offset info back to ''external storage''
    * we traverse each rdd's each partition's OffsetRange
    * and write its consumed detail info back to ''external storage''
    *
    * @param stream DStream[String] the data streaming from kafka to Spark StreamingContext
    **/
  def kafkaDStreamCommit(stream: InputDStream[ConsumerRecord[String, String]]) = {
    stream.foreachRDD { eachRDD =>
      // first , we get the array of OffsetRange by calling eachRDD's
      val offsetRangeArray: Array[OffsetRange] = eachRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      eachRDD.foreachPartition { eachRDDPartitionIter =>
        val partitionId = TaskContext.getPartitionId
        val offsetRangeByPartition: OffsetRange = offsetRangeArray(partitionId)
        syncRangeOffsetToExternalStorage(offsetRangeByPartition)
      }
    }
  }

  def main(args: Array[String]) = {

    val batchInterval: Int = 10
    val brokers: String = ""
    val conf = new SparkConf().setAppName("KafkaSparkStreamingExactlyOnceApp")

    // here we set the task only fail once , in case of stage and job retry
    // to produce duplicated messages
    conf.set("spark.task.maxFailures", "1")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(batchInterval))

    ssc.remember(Seconds(batchInterval * 2))

    var topicPartitonNum: Map[String, Int] = Map()
    topicPartitonNum += ("dasou-in" -> 5)
    // here we try multi-topics offset cache
    topicPartitonNum += ("dasou-stream" -> 5)

    println("[build] [KfakaDStreamInit] [begin]")
    val stream: InputDStream[ConsumerRecord[String, String]] = kafkaDStreamInit(ssc, brokers, topicPartitonNum, true)
    println("[build] [KfakaDStreamInit] [end]")


    //======serviceLogicOp(stream)
    println("[build] [streamLines] [begin]")
    val streamLines = stream.map(_.value)
    val resultRDD = serviceLogicOp(streamLines)
    println("[build] [streamLines] [end]")
    //======serviceLogicOp(stream)

    //=========== kafkaDStreamCommit(stream)
    println("[begin] [commit] kafkaDStream offset")
    kafkaDStreamCommit(stream)
    println("[end] [commit] kafkaDStream offset")
    //=========== kafkaDStreamCommit(stream)

    val hdfsPath: String = "/app/business/haichuan/cbc/spark/results"
    println(s"[begin] [saveAsTextFiles] =${hdfsPath}")
    resultRDD.saveAsTextFiles(s"${hdfsPath}")
    println(s"[end] [saveAsTextFiles]=${hdfsPath}")
    ssc.start()
    ssc.awaitTermination()
    /** *
      * nohup.out :
      * 18/10/03 00:29:22 INFO Executor: Running task 1.0 in stage 1.0 (TID 6)
      * 18/10/03 00:29:22 INFO Executor: Running task 0.0 in stage 1.0 (TID 5)
      * 18/10/03 00:29:22 INFO Executor: Running task 3.0 in stage 1.0 (TID 8)
      * 18/10/03 00:29:22 INFO Executor: Running task 4.0 in stage 1.0 (TID 9)
      * 18/10/03 00:29:22 INFO Executor: Running task 2.0 in stage 1.0 (TID 7)
      * 18/10/03 00:29:22 INFO KafkaRDD: Computing topic dasou-in, partition 1 offsets 1195 -> 33409
      * 18/10/03 00:29:22 INFO KafkaRDD: Computing topic dasou-in, partition 3 offsets 1191 -> 33403
      * 18/10/03 00:29:22 INFO KafkaRDD: Computing topic dasou-in, partition 2 offsets 1195 -> 33408
      * 18/10/03 00:29:22 INFO KafkaRDD: Computing topic dasou-in, partition 0 offsets 1196 -> 33410
      * 18/10/03 00:29:22 INFO KafkaRDD: Computing topic dasou-in, partition 4 offsets 1196 -> 33411
      *
      */
    /**
      * zk cache1:
      * get /spark/spark/kafka/dasou-in/2
      * {"partition":2,"topic":"dasou-in","untilOffset":1195,"fromOffset":0}
      * cZxid = 0x40000146a
      * ctime = Tue Oct 02 21:27:36 CST 2018
      * mZxid = 0x4000014e3
      * mtime = Tue Oct 02 22:59:22 CST 2018
      * pZxid = 0x40000146a
      * cversion = 0
      * dataVersion = 1
      * aclVersion = 0
      * ephemeralOwner = 0x0
      * dataLength = 68
      * numChildren = 0
      *
      * zk cache2:
      * get /spark/terry/kafka/dasou-in/2
      * {"partition":2,"topic":"dasou-in","untilOffset":33408,"fromOffset":1195}
      * cZxid = 0x40000146a
      * ctime = Tue Oct 02 21:27:36 CST 2018
      * mZxid = 0x40000153d
      * mtime = Wed Oct 03 00:29:21 CST 2018
      * pZxid = 0x40000146a
      * cversion = 0
      * dataVersion = 2
      * aclVersion = 0
      * ephemeralOwner = 0x0
      * dataLength = 72
      * numChildren = 0
      **/

    // ssc.stop(false, true)
  }
}
