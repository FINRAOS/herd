package org.finra.catalog

import org.apache.spark.sql.herd.{DefaultSource, HerdApi}

import org.finra.herd.sdk.invoker.ApiClient

/**
 * A thin wrapper that hides the actual implementations of HerdApi and ApiClient so we can easily mock them in the unit tests
 *
 */
class HerdApiWrapper {
  private var herdApi : HerdApi = null
  private var apiClient : ApiClient = null

  /**
   * Constructor for the wrapper class that creates HerdApi and ApiClient instances
   *
   * @param dataSource  the data source
   * @param baseRestUrl the base url for HERD service
   * @param username    username of the account to access HERD
   * @param password    password of the account to access HERD
   */
  def this(dataSource: DefaultSource.type, baseRestUrl: String, username: String, password: String) {
    this()
    this.herdApi = dataSource.defaultApiClientFactory(baseRestUrl, Some(username), Some(password))

    this.apiClient = new ApiClient()
    apiClient.setBasePath(baseRestUrl)
    List(username).foreach(username => apiClient.setUsername(username))
    List(password).foreach(password => apiClient.setPassword(password))
  }

  /**
   * Returns the HERD API
   *
   * @return the herd api instance
   */
  def getHerdApi(): HerdApi = {
    this.herdApi
  }

  /**
   * Returns the API Client
   *
   * @return the api client instance
   */
  def getApiClient(): ApiClient = {
    this.apiClient
  }
}
