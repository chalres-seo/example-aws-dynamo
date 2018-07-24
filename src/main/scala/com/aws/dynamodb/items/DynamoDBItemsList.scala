package com.aws.dynamodb.items

object DynamoDBItemsList extends Enumeration {
  type Names = DynamoDBItemsList.Value

  val ExampleA: Names = Value("ExampleItemA")
  val ExampleB: Names = Value("ExampleItemB")

  private val valueList = Seq(ExampleA, ExampleB)
}