package org.finra.herd.dao.helper;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * ElasticsearchRestClientHelper is a class that contains helper methods that use the Elasticsearch REST client.
 * <p>
 * We are using the RestClient instead of the TransportClient because the RestClient will be supported by the Amazon Elasticsearch Service, whereas the
 * TransportClient will not be supported by the Amazon Elasticsearch Service.
 */
@Component
public class ElasticsearchRestClientHelper {
    /**
     * The Elasticsearch rest client object used to connect to Elasticsearch to make process REST requests
     * This object will be dependency injected by Spring
     */
    private final RestClient restClient;

    /**
     * Autowired constructor for the ElasticsearchRestClientHelper.
     *
     * @param restClient the rest client object used to connect to Elasticsearch
     */
    @Autowired
    public ElasticsearchRestClientHelper(RestClient restClient) {
        this.restClient = restClient;
    }

    /**
     * Private method to perform an Elasticsearch REST request.
     *
     * @param method the request method
     * @param endpoint the request endpoint
     * @param params the request parameters
     * @param requestBodyJsonString the request body
     *
     * @return the response string
     */
    public String performRequest(final String method, final String endpoint, final Map<String, String> params, final String requestBodyJsonString) {
        final String responseString;
        Response response;

        try {
            if( StringUtils.isNotEmpty(requestBodyJsonString)) {
                // Create the HttpEntity for the request
                final HttpEntity httpEntity = new NStringEntity(requestBodyJsonString, ContentType.APPLICATION_JSON);
                // Perform the request on the Elasticsearch REST client, providing the REST method, endpoint, params, and body.
                response = restClient.performRequest(method, endpoint, params, httpEntity);
            } else {
                response = restClient.performRequest(method, endpoint, params);
            }

            // Use the entity utils to get the response string from the response object.
            responseString = EntityUtils.toString(response.getEntity());
        }
        catch (final IOException ioException) {
            // Wrap in an ElasticsearchException which extends RuntimeException
            throw new ElasticsearchException(ioException);
        }

        return responseString;
    }

    /**
     * Private method to perform an Elasticsearch REST request.
     *
     * @param method the request method
     * @param endpoint the request endpoint
     * @param params the request parameters
     *
     * @return the response string
     */
    public String performRequest(final String method, final String endpoint, final Map<String, String> params) {
        return performRequest(method, endpoint, params, null);
    }
}