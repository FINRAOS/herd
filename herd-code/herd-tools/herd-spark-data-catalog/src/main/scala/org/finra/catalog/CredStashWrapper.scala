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
package org.finra.catalog

import java.util

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.kms.AWSKMSClient
import com.jessecoyle.{CredStashBouncyCastleCrypto, JCredStash}

/**
 * A thin wrapper that hides the actual implementation of credStash so we can easily mock it in the unit tests
 *
 */
class CredStashWrapper {
  private var credStash : JCredStash = null

  /**
   * Constructor for the wrapper class that creates a JCredStash instance
   *
   * @param amazonDynamoDBClient amazon DynamoDB Client
   * @param awskmsClient amazon KMS client
   */
  def this(amazonDynamoDBClient: AmazonDynamoDBClient, awskmsClient: AWSKMSClient) {
    this()
    this.credStash = new JCredStash("credential-store", amazonDynamoDBClient, awskmsClient, new CredStashBouncyCastleCrypto)
  }

  /**
   * Returns the password of the credential name using credStash
   *
   * @param credential  the credential name
   * @param context the credential context used by credStash
   * @return the password
   */
  def getSecret(credential : String, context : util.HashMap[String, String]) : String = {
    return this.credStash.getSecret(credential, context)
  }

}
