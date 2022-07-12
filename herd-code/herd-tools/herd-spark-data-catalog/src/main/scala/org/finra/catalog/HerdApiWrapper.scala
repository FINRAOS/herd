package org.finra.catalog

import org.apache.spark.sql.herd.{DefaultSource, HerdApi, OAuthTokenProvider}

import org.finra.herd.sdk.invoker.{ApiClient}

/**
 * A thin wrapper that hides the actual implementations of HerdApi and ApiClient so we can easily mock them in the unit tests
 *
 */
class HerdApiWrapper {
  private var herdApi : HerdApi = null
  private var apiClient : ApiClient = null
  private var username: String = null
  private var password: String = null
  private var accessTokenUrl: String = null

  /**
   * Constructor for the wrapper class that creates HerdApi and ApiClient instances
   *
   * @param dataSource  the data source
   * @param baseRestUrl the base url for HERD service
   * @param username    username of the account to access HERD
   * @param password    password of the account to access HERD
   */
  def this(dataSource: DefaultSource.type, baseRestUrl: String, username: String, password: String, accessTokenUrl: String) {
    this()
    this.username = username
    this.password = password
    this.accessTokenUrl = accessTokenUrl

    this.apiClient = new ApiClient()
    apiClient.setBasePath(baseRestUrl)

    if (accessTokenUrl != null && accessTokenUrl.trim.nonEmpty) {
      apiClient.setAccessToken(OAuthTokenProvider.getAccessToken(username, password, accessTokenUrl))
    } else {
      List(username).foreach(username => apiClient.setUsername(username))
      List(password).foreach(password => apiClient.setPassword(password))
    }

    this.herdApi = dataSource.defaultApiClientFactory(baseRestUrl, Some(username), Some(password), Some(accessTokenUrl))
  }

  /**
   * Returns the HERD API
   *
   * @return the herd api instance
   */
  def getHerdApi(): HerdApi = {
    if (accessTokenUrl != null && accessTokenUrl.trim.nonEmpty) {
      this.herdApi.refreshApiClient(OAuthTokenProvider.getAccessToken(this.username, this.password, this.accessTokenUrl));
    }
    this.herdApi
  }

  /**
   * Returns the API Client
   *
   * @return the api client instance
   */
  def getApiClient(): ApiClient = {
    if (this.accessTokenUrl != null && this.accessTokenUrl.trim.nonEmpty) {
      this.apiClient.setAccessToken(OAuthTokenProvider.getAccessToken(username, password, accessTokenUrl))
    }
    this.apiClient
  }
}
