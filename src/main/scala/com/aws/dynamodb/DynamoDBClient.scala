package com.aws.dynamodb

import java.util.concurrent.ConcurrentHashMap

import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Table}
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.aws.credentials.CredentialsFactory
import com.typesafe.scalalogging.LazyLogging
import com.utils.{AppConfig, DynamoRetry}

import scala.collection.concurrent
import scala.collection.JavaConverters._

import scala.util.{Failure, Success, Try}

case class DynamoDBClient(awsProfile: String, awsRegion: String) extends LazyLogging {
  private[this] val _dynamoDBClient: AmazonDynamoDB = DynamoDBClient.getDynamoDBClient(awsProfile, awsRegion)
  private[this] val _dynamoDB = new DynamoDB(_dynamoDBClient)

  /**
    * Get table description.
    *
    * @param tableName unchecked table name.
    *
    * @throws com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException table is not exist.
    *
    * @return
    */
  @throws(classOf[ResourceNotFoundException])
  @throws(classOf[InternalServerErrorException])
  def getTableDesc(tableName: String): Try[TableDescription] = {
    logger.debug(s"get table description. name: $tableName")

    DynamoRetry.retry("get table description.")(_dynamoDB.getTable(tableName).describe())
  }

  /**
    * Get table status:
    *   CREATING,
    *   UPDATING,
    *   DELETING,
    *   ACTIVE,
    *   NOT_EXIST (custom add)
    *
    * @param tableName unchecked table name.
    *
    * @see [[TableStatus]]
    *
    * @return
    */
  def getTableStatus(tableName: String): String = {
    logger.debug(s"get table status. name: $tableName")

    this.getTableDesc(tableName) match {
      case Success(tableDescription) =>
        logger.debug(s"table: $tableName, status: ${tableDescription.getTableStatus}")
        tableDescription.getTableStatus
      case Failure(_) =>
        logger.debug(s"table: $tableName, status: NOT_EXIST")
        "NOT_EXIST"
    }
  }

  def isTableExist(tableName: String): Boolean = {
    logger.debug(s"check table exist. name: $tableName")

    this.getTableStatus(tableName) match {
      case "NOT_EXIST" => false
      case _ => true
    }
  }

  def isTableNotExist(tableName: String): Boolean = {
    !this.isTableExist(tableName)
  }

  def createTableAndWaitActive(tableName: String,
                  hashKeyName: String,
                  hashKeyAttributeType: ScalarAttributeType,
                  rangeKeyName: String,
                  rangeKeyAttributeType: ScalarAttributeType,
                  attributes: AttributeDefinition*): Boolean = {
    logger.debug(s"create table and wait for active. name: $tableName, hashKey-name: $hashKeyName, rangeKey-name: $rangeKeyName")

    val createTableRequest = new CreateTableRequest()
      .withTableName(tableName)
      .withKeySchema(DynamoDBClient.getKeySchemaElement(hashKeyName, KeyType.HASH))
      .withKeySchema(DynamoDBClient.getKeySchemaElement(rangeKeyName, KeyType.RANGE))
      .withAttributeDefinitions(DynamoDBClient.getAttributeDefinition(hashKeyName, hashKeyAttributeType))
      .withAttributeDefinitions(DynamoDBClient.getAttributeDefinition(rangeKeyName, rangeKeyAttributeType))
      .withProvisionedThroughput(DynamoDBClient.getProvisionedThroughput)
      .withAttributeDefinitions(attributes:_*)

    val createTable: Table = _dynamoDB.createTable(createTableRequest)
    logger.debug(s"table created. name: $tableName, status: ${createTable.describe().getTableStatus}")

    logger.debug(s"wait for table to be active. name: $tableName")
    createTable.waitForActive()
    logger.debug(s"create complete. name: $tableName, status: ${createTable.describe().getTableStatus}")

    this.isTableExist(tableName)
  }

  def deleteTableAndWait(tableName: String): Boolean = {
    logger.debug(s"delete table and wait. name: $tableName")

    if (this.isTableExist(tableName)) {
      val deleteTable = _dynamoDB.getTable(tableName)
      deleteTable.delete()
      logger.debug(s"table deleted. name: $tableName, status: ${deleteTable.describe().getTableStatus}")

      logger.debug(s"wait for table delete. name: $tableName")
      deleteTable.waitForDelete()
      logger.debug(s"delete complete. name: $tableName, status: ${this.getTableStatus(tableName)}")
    }

    this.isTableNotExist(tableName)
  }

  def getTableList(): Vector[Table] = {
    logger.debug("get table list.")

    _dynamoDB.listTables().asScala.toVector
  }

  def getTableNameList(): Vector[String] = {
    this.getTableList().map(_.getTableName)
  }
}

object DynamoDBClient {
  private[this] val _dynamoDBClientBuilder: AmazonDynamoDBClientBuilder = AmazonDynamoDBClientBuilder.standard()

  private[this] val _awsProfile: String = AppConfig.awsProfile
  private[this] val _awsRegion: String = AppConfig.awsRegion

  private[this] val _provisionedThroughput = new ProvisionedThroughput()
    .withReadCapacityUnits(AppConfig.awsDynamoThroughputRead)
    .withWriteCapacityUnits(AppConfig.awsDynamoThroughputWrite)

  private[this] val _dynamoDBClientList: concurrent.Map[String, AmazonDynamoDB] =
    new ConcurrentHashMap[String, AmazonDynamoDB]().asScala

  def apply(awsProfile: String, awsRegion: String): DynamoDBClient = {
    new DynamoDBClient(awsProfile, awsRegion)
  }

  def apply(): DynamoDBClient = this.apply(_awsProfile, _awsRegion)

  def getDynamoDBClient(awsProfile: String, awsRegion: String): AmazonDynamoDB = {
    _dynamoDBClientList.getOrElseUpdate(awsProfile + "::" + awsRegion, this.createDynamoDBClient(awsProfile, awsRegion))
  }

  def getDynamoDBClient: AmazonDynamoDB = {
    _dynamoDBClientList.getOrElseUpdate(_awsProfile + "::" + _awsRegion, this.createDynamoDBClient(_awsProfile, _awsRegion))
  }

  private[this] def createDynamoDBClient(awsProfile: String, awsRegion: String): AmazonDynamoDB = {
    _dynamoDBClientBuilder
      .withCredentials(CredentialsFactory.getCredentialsProvider(awsProfile))
      .withRegion(awsRegion)
      .build()
  }

  private def getProvisionedThroughput: ProvisionedThroughput = this._provisionedThroughput

  def getAttributeDefinition(attributeName: String, attributeType: ScalarAttributeType): AttributeDefinition = {
    new AttributeDefinition()
      .withAttributeName(attributeName)
      .withAttributeType(attributeType)
  }

  def getKeySchemaElement(keyName: String, keyType: KeyType): KeySchemaElement = {
    new KeySchemaElement()
      .withAttributeName(keyName)
      .withKeyType(keyType)
  }
}
