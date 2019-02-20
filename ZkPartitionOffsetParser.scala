package com.spark.exactlyonce.zk

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by terry on 2018/10/1.
  */
object ZkPartitionOffsetParser {

  def fromTopicPartition(topicPartition: TopicPartition): String = {
    val jsonObj = new JSONObject
    jsonObj.put("topic", topicPartition.topic)
    jsonObj.put("partition", topicPartition.partition)

    jsonObj.toString
  }

  def toTopicPartition(str: String): TopicPartition = {
    val jsonObj: JSONObject = JSON.parseObject(str, classOf[JSONObject])
    val topic: String = jsonObj.getString("topic")
    val partition: Int = jsonObj.getInteger("partition")
    new TopicPartition(topic, partition)
  }

  def fromOffsetRange(offsetRange: OffsetRange): String = {
    val jsonObj = new JSONObject
    jsonObj.put("topic", offsetRange.topic)
    jsonObj.put("partition", offsetRange.partition)
    jsonObj.put("fromOffset", offsetRange.fromOffset)
    jsonObj.put("untilOffset", offsetRange.untilOffset)
    jsonObj.toString
  }

  def toOffsetRange(str: String): OffsetRange = {
    // create(topic : scala.Predef.String, partition : scala.Int, fromOffset : scala.Long, untilOffset : scala.Long)
    val jsonObj: JSONObject = JSON.parseObject(str, classOf[JSONObject])
    val topic: String = jsonObj.getString("topic")
    val partition: Int = jsonObj.getInteger("partition")
    val fromOffset: Long = jsonObj.getLong("fromOffset")
    val untilOffset: Long = jsonObj.getLong("untilOffset")
    OffsetRange.create(topic, partition, fromOffset, untilOffset)
  }

  // All tests pass
  def main(args:Array[String]) = {
    val topic = "dasou-stream"
    val partitionId:Int = 4

    val topicPartition:TopicPartition = new TopicPartition(topic, partitionId)
    val topicPartitionStr:String = fromTopicPartition(topicPartition)
    println(s"[TopicPartition] to Str =${topicPartitionStr}")

    val topicPartitionFromStr:TopicPartition = toTopicPartition(topicPartitionStr)
    println(s"is topicPartitionFromStr ${topicPartitionFromStr.isInstanceOf[TopicPartition]}")



    val offsetRangeObj:OffsetRange = OffsetRange.create(topic, partitionId, 0L, 0L)
    val offsetRangeStr:String = fromOffsetRange(offsetRangeObj)

    println(s"[OffsetRange] to Str=${offsetRangeStr}")

    val offsetRangeFromStr:OffsetRange = toOffsetRange(offsetRangeStr)
    println(s"is offsetRangeFromStr ${offsetRangeFromStr.isInstanceOf[OffsetRange]}")
  }
}
