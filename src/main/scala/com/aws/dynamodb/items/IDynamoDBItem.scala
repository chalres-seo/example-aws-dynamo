package com.aws.dynamodb.items

trait IDynamoDBItem {
  def getHashKey: Option[AnyRef]
  def setHashKey(o: Any): Unit
  def getRangeKey: Option[AnyRef]
  def setRangeKey(o: Any): Unit
  def getUpdateTime: String
  def setUpdateTime(updateTime: String): Unit
}
