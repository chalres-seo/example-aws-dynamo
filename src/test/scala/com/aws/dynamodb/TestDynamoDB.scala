package com.aws.dynamodb

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.aws.dynamodb.items.{DynamoDBItemsList, ExampleItemA, ExampleItemB, IDynamoDBItem}
import com.aws.dynamodb.tables.DynamoDBTable
import com.typesafe.scalalogging.LazyLogging
import org.junit.{Assert, FixMethodOrder, Test}
import org.junit.runners.MethodSorters
import org.hamcrest.CoreMatchers._

import scala.collection.immutable
import scala.util.{Success, Try}

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class TestDynamoDB extends LazyLogging {

  private val testTableName: String = "test-table"

  private val testMapperTableA: String = "exampleA"
  private val testMapperTableB: String = "exampleB"

  private val testRecordCount = 10

  private val dynamoDBClient: DynamoDBClient = DynamoDBClient()

  def setUp() = {
    if (dynamoDBClient.isTableExist(testTableName)) {
      dynamoDBClient.deleteTableAndWait(testTableName)
    }

    if (dynamoDBClient.isTableNotExist(testMapperTableA)) {
      dynamoDBClient.createTableAndWaitActive(testMapperTableA,
        "idA",
        ScalarAttributeType.S,
        "indexA",
        ScalarAttributeType.N)
    }

    if (dynamoDBClient.isTableNotExist(testMapperTableB)) {
      dynamoDBClient.createTableAndWaitActive(testMapperTableB,
        "idB",
        ScalarAttributeType.S,
        "indexB",
        ScalarAttributeType.N)
    }
  }

  def cleanUp() = {
    if (dynamoDBClient.isTableExist(testTableName)) {
      dynamoDBClient.deleteTableAndWait(testTableName)
    }

    if (dynamoDBClient.isTableExist(testMapperTableA)) {
      dynamoDBClient.deleteTableAndWait(testMapperTableA)
    }

    if (dynamoDBClient.isTableExist(testMapperTableB)) {
      dynamoDBClient.deleteTableAndWait(testMapperTableB)
    }
  }

  @Test
  def test99CleanUp: Unit = {
    this.setUp()
  }

  @Test
  def test01TestCreateAndDeleteTable: Unit = {
    this.setUp()

    Assert.assertThat(dynamoDBClient.awsProfile, is("default"))
    Assert.assertThat(dynamoDBClient.awsRegion, is("ap-northeast-2"))

    Assert.assertThat(dynamoDBClient.getTableNameList().contains(testTableName), is(false))

    Assert.assertThat(dynamoDBClient.createTableAndWaitActive(
      testTableName,"hashKey",ScalarAttributeType.S,"rangeKey",ScalarAttributeType.S),
      is(true))
    Assert.assertThat(dynamoDBClient.getTableNameList().contains(testTableName), is(true))

    Assert.assertThat(dynamoDBClient.deleteTableAndWait(testTableName), is(true))
    Assert.assertThat(dynamoDBClient.getTableList().contains(testTableName), is(false))
  }


  @Test
  def test02TestDynamoDBMapper: Unit = {
    this.setUp()

    val exampleItemA: DynamoDBTable[ExampleItemA] = DynamoDBTable.getInstance(classOf[ExampleItemA])
    val exampleItemB: DynamoDBTable[ExampleItemB] = DynamoDBTable.getInstance(classOf[ExampleItemB])

    Assert.assertThat(Try(DynamoDBTable.getInstance(classOf[IDynamoDBItem])).failed.get.getMessage.startsWith("requirement failed"), is(true))

    val testExampleItemARecord: immutable.Seq[ExampleItemA] = (1 to testRecordCount).map(i => {
      new ExampleItemA("idA-" + i, i, "valueA-" + i)
    })

    val testExampleItemBRecord: immutable.Seq[ExampleItemB] = (1 to testRecordCount).map(i => {
      new ExampleItemB("idB-" + i, i, "valueB-" + i)
    })

    Assert.assertThat(exampleItemA.save(testExampleItemARecord.toVector), is(true))
    Assert.assertThat(exampleItemB.save(testExampleItemBRecord.toVector), is(true))

    exampleItemA.scanAll match {
      case Some(list) =>
        list.foreach(ele => logger.debug(ele.toString))
        Assert.assertThat(list.size, is(testRecordCount))
      case None => println("none")
    }

    exampleItemB.scanAll match {
      case Some(list) =>
        list.foreach(ele => logger.debug(ele.toString))
        Assert.assertThat(list.size, is(testRecordCount))
      case None => println("none")
    }
  }
}
