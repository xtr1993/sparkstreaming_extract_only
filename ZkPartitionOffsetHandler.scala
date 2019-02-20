package com.spark.exactlyonce.zk

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by terry on 2018/10/1.
  *
  * We will organize the metadata of kafka offset in this structured:
  *
  * /${zkRootPath}/
  * -path(String): topic/${partition-id}
  * -value(metadata:String): RangeOffset.{topic:String, partition:Int, fromOffset:Long, untilOffset:Long}
  *
  * /${zkRootPath}/
  * -path(String):topic/
  * -value (metadata:String): {"topic":${topicName}, "partitionNum":${partitionNum}}
  */

object ZkPartitionOffsetHandler {

  val zkBrokerList: String = ""
  val sessionTimeout: Int = 5000
  val zkRootPath = "/aimer/spark/kafka"


  def isTopicCached(topic: String): Boolean = {
    ZkUtil.conn(zkBrokerList, sessionTimeout)
    val isTopicPathExists = ZkUtil.isPathExsits(s"${zkRootPath}/${topic}")
    ZkUtil.close
    isTopicPathExists
  }

  def addRangeOffset(offsetRange: OffsetRange) = {
    ZkUtil.conn(zkBrokerList, sessionTimeout)
    ZkUtil.create(s"${zkRootPath}/${offsetRange.topic}/${offsetRange.partition}",
      ZkPartitionOffsetParser.fromOffsetRange(offsetRange))
    ZkUtil.close
  }

  def deleteRangeOffset(offsetRange: OffsetRange) = {
    ZkUtil.conn(zkBrokerList, sessionTimeout)
    ZkUtil.delete(s"${zkRootPath}/${offsetRange.topic}/${offsetRange.partition}")
    ZkUtil.close

  }

  def updateRangeOffset(offsetRange: OffsetRange) = {
    ZkUtil.conn(zkBrokerList, sessionTimeout)
    println("[ZkPartitionOffsetHandler] [updateRangeOffsets] [begin]")
    ZkUtil.update(s"${zkRootPath}/${offsetRange.topic}/${offsetRange.partition}",
      ZkPartitionOffsetParser.fromOffsetRange(offsetRange))
    println("[ZkPartitionOffsetHandler] [updateRangeOffsets] [end]")
    ZkUtil.close
  }


  def deleteTopicPartiton(topic: String) = {
    ZkUtil.conn(zkBrokerList, sessionTimeout)
    val topicPath = s"${zkRootPath}/${topic}"
    val childrenDir = ZkUtil.getChildren(s"${zkRootPath}/${topic}")
    for (childDir <- childrenDir) {
      val subPath = s"${topicPath}/${childDir}"
      println(s"delete subPath=${subPath}")
      ZkUtil.delete(subPath)
    }
    ZkUtil.delete(topicPath)
    println(s"finally delete topicPath=${topicPath}")
    ZkUtil.close()
  }

  def deleteTopicPartition(topicPartition: TopicPartition) = {
    deleteTopicPartiton(topicPartition.topic())
  }

  def deleteTopicPartition(offsetRange: OffsetRange) = {
    deleteTopicPartiton(offsetRange.topic)
  }


  def addTopicPartitionNum(topic: String, partitionNum: Int) = {
    val path = s"${zkRootPath}/${topic}"
    ZkUtil.conn(zkBrokerList, sessionTimeout)
    val jsonObj: JSONObject = new JSONObject
    jsonObj.put("topic", topic)
    jsonObj.put("partitionNum", partitionNum)
    if (ZkUtil.isPathExsits(path)) {
      println(s"[ZkPartitionOffsetHandler] path=${path} already exists")
      ZkUtil.update(path, jsonObj.toString)
    } else {
      println(s"[ZkPartitionOffsetHandler] path=${path} does not exist")
      ZkUtil.create(path, jsonObj.toString)
      for (partitinId: Int <- 0 until partitionNum) {
        val rangeOffset: OffsetRange = OffsetRange.create(topic, partitinId, 0, 0)
        println(s"build path for ${path}/${rangeOffset.partition}" +
          s" with data=${ZkPartitionOffsetParser.fromOffsetRange(rangeOffset)}")
        ZkUtil.create(s"${zkRootPath}/${rangeOffset.topic}/${rangeOffset.partition}",
          ZkPartitionOffsetParser.fromOffsetRange(rangeOffset))
      }
    }
    ZkUtil.close
  }

  def readTopicPartitonNum(topic: String): Int = {
    val path = s"${
      zkRootPath
    }/${
      topic
    }"
    ZkUtil.conn(zkBrokerList, sessionTimeout)
    val jSONObject = JSON.parseObject(ZkUtil.get(path), classOf[JSONObject])
    val partitionNum: Int = jSONObject.getInteger("partitionNum")
    ZkUtil.close
    partitionNum
  }


  def readRangeOffset(topic: String, partition: Int): OffsetRange = {
    ZkUtil.conn(zkBrokerList, sessionTimeout)
    val offsetRange: OffsetRange =
      ZkPartitionOffsetParser.toOffsetRange(ZkUtil.get(s"${
        zkRootPath
      }/${
        topic
      }/${
        partition
      }"))
    ZkUtil.close
    offsetRange
  }

  def readRangeOffset(topicPartiton: TopicPartition): OffsetRange = {
    ZkUtil.conn(zkBrokerList, sessionTimeout)
    val offsetRange: OffsetRange =
      ZkPartitionOffsetParser.
        toOffsetRange(ZkUtil.get(s"${
          zkRootPath
        }/${
          topicPartiton.topic
        }/${
          topicPartiton.partition
        }"))
    ZkUtil.close
    offsetRange
  }


  // All tests pass
  def main(args: Array[String]) = {

    val topic: String = "dasou-stream"
    val topicPartitionNum: Int = 5

    val zkHandler = ZkPartitionOffsetHandler

    // ====== isTopicCached
    var ret = zkHandler.isTopicCached(topic)
    println(s"is topic=${topic} cached=${ret}")
    // ====== isTopicCached


    // ====== addTopicPartitionNum
    zkHandler.addTopicPartitionNum(topic, topicPartitionNum)
    /** *
      * get /aimer/spark/kafka/dasou-stream/4
      * {"partition":4,"topic":"dasou-stream","untilOffset":0,"fromOffset":0}
      * */
    /**
      * get /aimer/spark/kafka/dasou-stream
      * {"topic":"dasou-stream","partitionNum":5}
      */
    // ====== addTopicPartitionNum


    // ====== addRangeOffset
    val offsetRange: OffsetRange = OffsetRange.create("dasou-stream", 5, 0, 0)
    zkHandler.addRangeOffset(offsetRange)
    /**
      * get /aimer/spark/kafka/dasou-stream/5
      * {"partition":5,"topic":"dasou-stream","untilOffset":0,"fromOffset":0}
      */
    // ====== addRangeOffset


    // ====== updateRangeOffet
    val offsetRangeUpdated: OffsetRange = OffsetRange.create("dasou-stream", 5, 100L, 2000L)
    zkHandler.updateRangeOffset(offsetRangeUpdated)
    /** get /aimer/spark/kafka/dasou-stream/5
      * {"partition":5,"topic":"dasou-stream","untilOffset":2000,"fromOffset":100}
      */
    // ====== updateRangeOffet


    // ====== deleteRangeOffset
    zkHandler.deleteRangeOffset(offsetRange)
    /**
      * get /aimer/spark/kafka/dasou-stream/
      *
      * 3   2   1   0   4
      */
    // ====== deleteRangeOffset


    // ====== deleteTopicPartition(offsetRange:OffsetRange)
    zkHandler.deleteTopicPartition(offsetRange)
    // ====== deleteTopicPartition(offsetRange:OffsetRange)

  }
}
