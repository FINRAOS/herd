/*
* Copyright 2015 herd contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.spark.sql.herd

import org.apache.log4j.Logger
import scala.util.{Failure, Success, Try}

import org.finra.herd.sdk.invoker.ApiException

/** A simple Interface that knows how to retry an action in case of error/failure */
trait Retry {

  val log: Logger

  private val MAX_TRIES = 3

  private val WAIT = 100

  def withRetry[T](block: => T): T = {
    var tries = 0

    def runRecursively[S](block: => S): S = {
      Try(block) match {
        case Success(result) => result
        case Failure(ex: ApiException) =>
          if (ex.getCode >= 400 && ex.getCode < 500) {
            log.error(s"Encountered fatal error from Herd, will not retry. Status code: ${ex.getCode}, error message: ${ex.toString}", ex)
            throw new ApiException(ex.getCode, s"Encountered fatal error from Herd, will not retry. Status code: ${ex.getCode}, error message: ${ex.toString}")
          }
          else if (tries < MAX_TRIES) {
            log.error(s"Herd returned an error, will retry ${MAX_TRIES - tries} times. Status code: ${ex.getCode}, error message: ${ex.toString}", ex)
            tries += 1
            Thread.sleep(WAIT * tries)
            runRecursively(block)
          } else {
            log.error(s"Retried $MAX_TRIES times - aborting. Status code: ${ex.getCode}, error message: ${ex.toString}", ex)
            throw new ApiException(ex.getCode, s"Retried 3 times. Aborting. Status code: ${ex.getCode}, error message: ${ex.toString}")
          }
      }
    }

    runRecursively(block)
  }
}
