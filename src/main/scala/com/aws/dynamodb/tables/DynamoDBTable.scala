package com.aws.dynamodb.tables

import java.util.concurrent.ConcurrentHashMap

import com.aws.dynamodb.items.{DynamoDBItemsList, IDynamoDBItem}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.concurrent
import scala.collection.JavaConverters._

/**
  * DynamoDB Table Class
  *
  */
case class DynamoDBTable[A <: IDynamoDBItem](clazz: Class[A]) extends LazyLogging with IDynamoDBTable[A] {

  override def load(item: A): Option[A] = {
    DynamoDBTableCommon.load(item)
  }

  def loadByKey(hashKey: String): Option[A] = {
    DynamoDBTableCommon.load(clazz, hashKey.asInstanceOf[AnyRef])
  }

  def loadByKey(hashKey: Int): Option[A] = {
    DynamoDBTableCommon.load(clazz, hashKey.asInstanceOf[AnyRef])
  }

  def loadByKey(hashKey: String, rangeKey: String): Option[A] =
    DynamoDBTableCommon.load(clazz, hashKey.asInstanceOf[AnyRef], rangeKey.asInstanceOf[AnyRef])
  def loadByKey(hashKey: String, rangeKey: Int): Option[A] =
    DynamoDBTableCommon.load(clazz, hashKey.asInstanceOf[AnyRef], rangeKey.asInstanceOf[AnyRef])
  def loadByKey(hashKey: Int, rangeKey: String): Option[A] =
    DynamoDBTableCommon.load(clazz, hashKey.asInstanceOf[AnyRef], rangeKey.asInstanceOf[AnyRef])
  def loadByKey(hashKey: Int, rangeKey: Int): Option[A] =
    DynamoDBTableCommon.load(clazz, hashKey.asInstanceOf[AnyRef], rangeKey.asInstanceOf[AnyRef])

  override def query(item: A): Option[Vector[A]] = DynamoDBTableCommon.query(clazz, item)

  override def save(item: A): Unit = DynamoDBTableCommon.save(item)
  override def save(items: Vector[A]): Boolean = DynamoDBTableCommon.save(items)

  override def delete(item: A): Unit = DynamoDBTableCommon.delete(item)
  override def delete(items: Vector[A]): Boolean = DynamoDBTableCommon.delete(items)

  override def scanAll: Option[Vector[A]] = DynamoDBTableCommon.scanAll(clazz)
}

object DynamoDBTable extends LazyLogging {
  private val dynamoDBPool: concurrent.Map[String, Any] = new ConcurrentHashMap[String, Any].asScala

  private val mapperList = DynamoDBItemsList.values.map(_.toString)

  def getInstance[A <: IDynamoDBItem](dynamoMapperClazz: Class[A]): DynamoDBTable[A] = {
    logger.debug(s"Get instance DynamoDBTable[${dynamoMapperClazz.getSimpleName}].")

    require(mapperList.contains(dynamoMapperClazz.getSimpleName),
      "incorrect dynamo db item mapper. mapper list: " + mapperList.mkString(", "))

    dynamoDBPool.getOrElseUpdate(dynamoMapperClazz.getSimpleName,
      this.createInstance(dynamoMapperClazz)).asInstanceOf[DynamoDBTable[A]]
  }

  private def createInstance[A <: IDynamoDBItem](clazz: Class[A]): DynamoDBTable[A] = {
    logger.debug(s"Create instance DynamoDBTable[${clazz.getSimpleName}].")

    DynamoDBTable(clazz)
  }
}