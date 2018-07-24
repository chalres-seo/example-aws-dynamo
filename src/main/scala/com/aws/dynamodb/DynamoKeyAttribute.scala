package com.aws.dynamodb

object DynamoKeyAttribute extends Enumeration {
  type Types = DynamoKeyAttribute.Value
  val String: Types = Value("S")
  val Number: Types = Value("N")
  val Binary: Types = Value("B")
}
