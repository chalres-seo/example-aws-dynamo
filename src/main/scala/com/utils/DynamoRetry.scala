package com.utils

import com.amazonaws.services.dynamodbv2.model.{InternalServerErrorException, LimitExceededException, ResourceInUseException, ResourceNotFoundException}
import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object DynamoRetry extends LazyLogging with Retry {
  private val backoffTimeInMillis: Long = AppConfig.backoffTimeInMillis
  private val attemptMaxCount: Int = AppConfig.attemptMaxCount

  def retry[T](msg: String)(fn: => T): Try[T] = Try(this.retryWithBackoff(attemptMaxCount, backoffTimeInMillis, msg)(fn))

  @throws(classOf[Exception])
  @tailrec
  def retryWithBackoff[T](attemptCount: Int, backoffMillis: Long, msg: String)(fn: => T): T = {
    Try(fn) match {
      case Success(result) => result

      case Failure(e: ResourceInUseException) =>
        logger.error(msg + " resource in use exception, the remaining attempts will be skipped.")
        logger.error(e.getMessage)
        throw e

      case Failure(e: ResourceNotFoundException) =>
        logger.error(msg + " resource not found exception, the remaining attempts will be skipped.")
        logger.error(e.getMessage)
        throw e

      case Failure(e: InternalServerErrorException) if attemptCount > 0 =>
        logger.error(msg + s" internal server error exception, retry with backoff. retry count remain: $attemptCount")
        logger.error(e.getMessage)

        this.backoff(backoffMillis)
        retryWithBackoff(attemptCount - 1, backoffMillis, msg)(fn)

      case Failure(e: LimitExceededException) if attemptCount > 0 =>
        logger.error(msg + s" limit exceeded exception, retry with backoff. retry count remain: $attemptCount")
        logger.error(e.getMessage)

        this.backoff(backoffMillis)
        retryWithBackoff(attemptCount - 1, backoffMillis, msg)(fn)

      case Failure(e: Exception) =>
        if (attemptCount > 0) {
          logger.error(msg + s" retry with backoff. retry count remain: $attemptCount")
          logger.error(e.getMessage)

          this.backoff(backoffMillis)
          retryWithBackoff(attemptCount - 1, backoffMillis, msg)(fn)
        } else {
          logger.error(msg + " attempts has been exceeded.")
          logger.error(e.getMessage)
          throw e
        }

      case Failure(t: Throwable) =>
        logger.error(s" unknown exception, the remaining attempts will be skipped.")
        logger.error(t.getMessage, t)
        throw t
    }
  }
}