package org.finra.herd.dao;

import io.searchbox.client.JestClient;
import io.searchbox.client.config.HttpClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * JestClientFactory
 */
@Component
public class JestClientFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JestClientFactory.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * Method to configure and build and return a JEST client.
     *
     * @return the configured JEST client
     */
    public JestClient getJestClient()
    {
        // Retrieve the configuration values used for setting up an Elasticsearch JEST client.
        final String hostname = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_HOSTNAME);
        final int port = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_PORT, Integer.class);
        final String scheme = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_REST_CLIENT_SCHEME);
        final String serverUri = String.format("%s://%s:%d", scheme, hostname, port);

        LOGGER.info("Elasticsearch REST Client Settings:  scheme={}, hostname={}, port={}, serverUri={}",
            scheme, hostname, port, serverUri);

        io.searchbox.client.JestClientFactory jestClientFactory = new io.searchbox.client.JestClientFactory();
        jestClientFactory.setHttpClientConfig(new HttpClientConfig.Builder(serverUri).multiThreaded(true).build());
        return jestClientFactory.getObject();
    }
}
