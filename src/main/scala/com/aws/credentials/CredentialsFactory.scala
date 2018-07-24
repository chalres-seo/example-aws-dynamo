package com.aws.credentials

import java.util.concurrent.ConcurrentHashMap

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.typesafe.scalalogging.LazyLogging
import com.utils.AppConfig

import scala.collection.JavaConverters._
import scala.collection.concurrent

object CredentialsFactory extends LazyLogging {
  private val credentialsProviderList: concurrent.Map[String, AWSCredentialsProvider] =
    new ConcurrentHashMap[String, AWSCredentialsProvider]().asScala

  def getDefaultCredentialsProvider: DefaultAWSCredentialsProviderChain = {
    logger.debug("get aws default credentials provider chain.")

    DefaultAWSCredentialsProviderChain.getInstance()
  }

  def getCredentialsProvider(profileName: String): AWSCredentialsProvider = {
    logger.debug(s"get aws profile com.aws.credentials provider, profile : $profileName")

    credentialsProviderList.getOrElseUpdate(profileName, this.createCredentialsProvider(profileName))
  }

  def getCredentialsProvider: AWSCredentialsProvider = {
    logger.debug("get aws default com.aws.credentials provider")

    credentialsProviderList.getOrElseUpdate(AppConfig.awsProfile, this.createCredentialsProvider)
  }

  private def createCredentialsProvider(profileName: String): ProfileCredentialsProvider = {
    logger.debug(s"create aws profile com.aws.credentials provider, profile : $profileName")

    new ProfileCredentialsProvider(profileName)
  }

  private def createCredentialsProvider: ProfileCredentialsProvider = {
    logger.debug("create aws default com.aws.credentials provider")

    this.createCredentialsProvider(AppConfig.awsProfile)
  }

  def refreshAllProvider(): Unit = {
    credentialsProviderList.foreach(_._2.refresh())
  }
}
