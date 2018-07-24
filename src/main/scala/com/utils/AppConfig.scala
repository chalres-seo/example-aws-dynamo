package com.utils

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  // read application.conf
  private val conf: Config = ConfigFactory.parseFile(new File("conf/application.conf")).resolve()

  // retry conf
  val backoffTimeInMillis: Long = conf.getLong("retry.backoffTimeInMillis")
  val attemptMaxCount: Int = conf.getInt("retry.attemptMaxCount")

  // aws account config
  val awsProfile: String = conf.getString("aws.profile")
  val awsRegion: String = conf.getString("aws.region")

  // aws dynamo config
  val awsDynamoThroughputRead: Long = conf.getLong("aws.dynamodb.throughputRead")
  val awsDynamoThroughputWrite: Long = conf.getLong("aws.dynamodb.throughputWrite")
}