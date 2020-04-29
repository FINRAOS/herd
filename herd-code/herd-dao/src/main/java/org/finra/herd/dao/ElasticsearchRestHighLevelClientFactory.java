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
package org.finra.herd.dao;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.dto.ConfigurationValue;

@Component
public class ElasticsearchRestHighLevelClientFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchRestHighLevelClientFactory.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * Builds and returns the Elasticsearch high level REST client.
     *
     * @return RestHighLevelClient The ElasticSearch high level REST client.
     */
    public RestHighLevelClient getRestHighLevelClient()
    {
        // Retrieve the configuration values needed to build a REST high level client.
        final String esRegionName = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_AWS_REGION_NAME);
        final String hostname = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_DOMAIN_REST_CLIENT_HOSTNAME_V2);
        final int port = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_DOMAIN_REST_CLIENT_PORT, Integer.class);
        final String scheme = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_DOMAIN_REST_CLIENT_SCHEME);
        final int socketTimeout = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_HIGH_LEVEL_CLIENT_SOCKET_TIMEOUT, Integer.class);
        final int connectTimeout = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_HIGH_LEVEL_CLIENT_CONNECTION_TIMEOUT, Integer.class);

        // Create and setup the AWS
        final DefaultAWSCredentialsProviderChain defaultAWSCredentialsProviderChain = new DefaultAWSCredentialsProviderChain();
        final AWSSigner awsSigner = new AWSSigner(defaultAWSCredentialsProviderChain, esRegionName, "es", () -> LocalDateTime.now(ZoneOffset.UTC));
        final AWSSigningRequestInterceptor awsSigningRequestInterceptor = new AWSSigningRequestInterceptor(awsSigner);

        // Create a new HTTP host object using the hostname, port, and scheme of the Elasticsearch domain.
        final HttpHost httpHost = new HttpHost(hostname, port, scheme);

        // Create a new REST client builder.
        final  RestClientBuilder restClientBuilder = RestClient.builder(httpHost);

        // Configure and set the request config callback.
        restClientBuilder.setRequestConfigCallback(
            requestConfigCallback -> requestConfigCallback.setSocketTimeout(socketTimeout).setConnectTimeout(connectTimeout));

        // Configure and set the http client config callback.
        restClientBuilder.setHttpClientConfigCallback(
            httpClientConfigCallback -> httpClientConfigCallback.addInterceptorLast(awsSigningRequestInterceptor));

        LOGGER.info("Creating new Elasticsearch REST high level client with hostname={}, port={}, and scheme={}.", hostname, port, scheme);

        // Return a new REST high level client to be used to connect and make requests to the Elasticsearch domain.
        return new RestHighLevelClient(restClientBuilder);
    }
}
