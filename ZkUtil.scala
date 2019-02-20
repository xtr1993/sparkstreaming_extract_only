package com.spark.exactlyonce.zk

/**
  * Created by terry on 2018/10/1.
  *
  * Zookeeper API CURD and path exists detector methods
  */

import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

object ZkUtil {

  var zkUtilHandler: ZooKeeper = _

  def conn(brokerList: String, sessionTimeoutMs: Int) = {
    zkUtilHandler = new ZooKeeper(brokerList, sessionTimeoutMs, new Watcher {
      override def process(watchedEvent: WatchedEvent) = {
        println(s"[ZkUtil] [event-info]=${watchedEvent.toString}")
      }
    })
  }

  def close() = {
    println("[ZkUtil] [close] [begin]")
    zkUtilHandler.close()
    println("[ZkUtil] [close] [done]")
  }


  def create(path: String, data: String) = {
    println(s"[ZkUtil] [create] [path]=${path} [data]=${data}")
    zkUtilHandler.create(path, data.getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    println(s"[ZkUtil] [create] [path]=${path} [data]=${data} done")
  }

  def delete(path: String) = {
    println(s"[ZkUtil] [delete] [path]=${path}")
    zkUtilHandler.delete(path, -1)
    println(s"[ZkUtil] [delete] [path]=${path} done")
  }

  def update(path: String, data: String) = {
    println(s"[ZkUtil] [update] [path]=${path} [data]=${data}")
    zkUtilHandler.setData(path, data.getBytes, -1)
    println(s"[ZkUtil] [update] [path]=${path} [data]=${data} done")
  }

  def get(path: String): String = {
    println(s"[ZkUtil] [get] [path]=${path}")
    val data: String = new String(zkUtilHandler.getData(path, true, null), "utf-8")
    println(s"[ZkUtil] [get] [path]=${path} done")
    data
  }

  def isPathExsits(path: String): Boolean = {
    println(s"[ZkUtil] [exists] [path]=${path}")
    val isExists: Boolean = zkUtilHandler.exists(path, true) match {
      case null => false
      case _ => true
    }
    println(s"[ZkUtil] [exists] [path]=${path} exists=${isExists}one")
    isExists
  }

  def getChildren(path:String) = {
    println(s"[ZkUtil] [getChildren] [path]=${path}")
    val children = zkUtilHandler.getChildren(path, false)
    println(s"[ZkUtil] [getChildren] [path]=${path} done")
    import scala.collection.JavaConverters._
    children.asScala
  }

  // All tests pass
  def main(args: Array[String]) = {

    val brokerList: String = ""
    val sessionTimeout: Int = 5000
    ZkUtil.conn(brokerList, sessionTimeout)

    val pathName: String = "/aimer/spark/kafka"
    var data: String = "{\"name\":\"metadata\"}"

    ZkUtil.create(pathName, data)

    val updateData: String = "updated metadata"
    ZkUtil.update(pathName, updateData)

    data = ZkUtil.get(pathName)
    println(s"pathName=${pathName}, data=${data}")

    ZkUtil.delete(pathName)
    ZkUtil.close()

  }

}
