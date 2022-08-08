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

import org.joda.time.DateTime

class AccessToken {
  var accessToken: String = null
  var scope: String = null
  var tokenType: String = null
  var expiresIn: DateTime = null

  def getAccessToken: String = accessToken

  def getScope: String = scope

  def getTokenType: String = tokenType

  def getExpiresIn: DateTime = expiresIn
}