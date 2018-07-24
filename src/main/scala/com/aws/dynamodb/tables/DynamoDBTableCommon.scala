package com.aws.dynamodb.tables

import java.util

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.datamodeling._
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, CreateTableRequest}
import com.aws.dynamodb.DynamoDBClient
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._

/**
  * DynamoDBTable Common
  *
  */
private[dynamodb] object DynamoDBTableCommon extends LazyLogging {

  private val dynamoDBClient: AmazonDynamoDB = DynamoDBClient.getDynamoDBClient
  private val dynamoDBMapper: DynamoDBMapper = new DynamoDBMapper(dynamoDBClient)

  private val scanAllExpression: DynamoDBScanExpression = new DynamoDBScanExpression()

  def load[A](keyItem: A): Option[A] = {
    logger.debug(s"Load item $keyItem")

    Option(dynamoDBMapper.load(keyItem))
  }

  def load[A](clazz: Class[A], hashKey: AnyRef): Option[A] = {
    logger.debug(s"load $clazz item by hash key : $hashKey")

    try {
      Option(dynamoDBMapper.load(clazz, hashKey))
    } catch {
      case e:Exception =>
        logger.error("DynamoDB item Load exception")
        logger.error(e.getMessage)
        None
    }
  }

  def load[A](clazz: Class[A], hashKey: AnyRef, rangeKey: AnyRef): Option[A] = {
    logger.debug(s"load $clazz item by hash key : $hashKey, range key : $rangeKey")

    try {
      Option(dynamoDBMapper.load(clazz, hashKey, rangeKey))
    } catch {
      case e:Exception =>
        logger.error("DynamoDB item Load exception")
        logger.error(e.getMessage)
        None
    }
  }

  def query[A](clazz: Class[A], item: A): Option[Vector[A]] = {
    logger.debug(s"query $clazz item $item")

    dynamoDBMapper.query(clazz, new DynamoDBQueryExpression[A].withHashKeyValues(item)) match {
      case list: PaginatedQueryList[A] => if (list.isEmpty) None else Option(list.asScala.toVector)
      case _ => None
    }
  }

  def query[A](clazz: Class[A], query: DynamoDBQueryExpression[A]): Option[Vector[A]] = {
    logger.debug(s"query $clazz item $query")

    dynamoDBMapper.query(clazz, query) match {
      case list: PaginatedQueryList[A] => if (list.isEmpty) None else Option(list.asScala.toVector)
      case _ => None
    }
  }

  def scan[A](clazz: Class[A], scan: DynamoDBScanExpression): Option[Vector[A]] = {
    logger.debug(s"scan $clazz item $scan")

    dynamoDBMapper.scan(clazz, scan) match {
      case list: PaginatedScanList[A] => if (list.isEmpty) None else Option(list.asScala.toVector)
      case _ => None
    }
  }

  def parallelScan[A](clazz: Class[A], scan: DynamoDBScanExpression, threadCount: Int): Option[Vector[A]] = {
    logger.debug(s"parallel scan $clazz item $scan, level $threadCount")

    dynamoDBMapper.parallelScan(clazz, scan, threadCount) match {
      case list: PaginatedParallelScanList[A] => if (list.isEmpty) None else Option(list.asScala.toVector)
      case _ => None
    }
  }

  def scanAll[A](clazz: Class[A]): Option[Vector[A]] = {
    logger.debug(s"scan all $clazz item")

    dynamoDBMapper.scan(clazz, this.scanAllExpression) match {
      case list: PaginatedScanList[A] => if (list.isEmpty) None else Option(list.asScala.toVector)
      case _ => None
    }
  }

  def parallelScanAll[A](clazz: Class[A], threadCount: Int): Option[Vector[A]] = {
    dynamoDBMapper.parallelScan(clazz, this.scanAllExpression, threadCount) match {
      case list: PaginatedParallelScanList[A] => if (list.isEmpty) None else Option(list.asScala.toVector)
      case _ => None
    }
  }

  def save[A](item: A): Unit = {
    logger.debug(s"save item $item")

    dynamoDBMapper.save[A](item)
  }
  def save[A](items: Seq[A]): Boolean = {
    logger.debug(s"save items head : ${items.head}, count : ${items.length}")

    dynamoDBMapper.batchSave(items.asJava) match {
      case failedList: util.List[DynamoDBMapper.FailedBatch] =>
        if (failedList.isEmpty) true
        else {
          failedList.asScala.foreach(failed => {
            logger.error(failed.getException.getMessage)
            logger.error(failed.getUnprocessedItems.asScala.mkString(","))
          })
          false
        }
      case _ => false
    }
  }

  def delete[A](item: A): Unit = {
    logger.debug(s"delete item $item")
    dynamoDBMapper.delete(item)
  }
  def delete[A](items: Seq[A]): Boolean = {
    logger.debug(s"delete items head : ${items.head}, count : ${items.length}")

    dynamoDBMapper.batchDelete(items.asJava) match {
      case failedList: util.List[DynamoDBMapper.FailedBatch] =>
        if (failedList.isEmpty) false
        else {
          failedList.asScala.foreach(failed => {
            logger.error(failed.getException.getMessage)
            logger.error(failed.getUnprocessedItems.asScala.mkString(","))
          })
          true
        }
      case _ => false
    }
  }

  def getCreateTableRequest[A](clazz: Class[A]): CreateTableRequest = {
    dynamoDBMapper.generateCreateTableRequest(clazz)
  }
}