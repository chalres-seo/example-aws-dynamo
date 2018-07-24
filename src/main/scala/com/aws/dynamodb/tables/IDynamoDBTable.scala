package com.aws.dynamodb.tables

import com.aws.dynamodb.items.IDynamoDBItem

/**
  * DynamoDB Table Interface
  *
  * @tparam A Dynamo Table Mapper Class (implement IDynamoDBItem)
  */
trait IDynamoDBTable[A <: IDynamoDBItem] {
  def load(item: A): Option[A]
  def query(item: A): Option[Vector[A]]
  def save(item: A): Unit
  def save(items: Vector[A]): Boolean
  def delete(item: A): Unit
  def delete(items: Vector[A]): Boolean
  def scanAll: Option[Vector[A]]
}