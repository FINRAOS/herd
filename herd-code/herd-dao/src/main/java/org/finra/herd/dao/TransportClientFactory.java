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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import com.amazonaws.ClientConfiguration;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.credstash.CredStash;
import org.finra.herd.dao.exception.CredStashGetCredentialFailedException;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.ElasticsearchSettingsDto;

/**
 * TransportClientFactory
 */
@Component
public class TransportClientFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransportClientFactory.class);

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private CredStashFactory credStashFactory;

    @Autowired
    private InputStreamFactory inputStreamFactory;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private PreBuiltTransportClientFactory preBuiltTransportClientFactory;

    /**
     * The Elasticsearch setting for cluster name
     */
    public static final String ELASTICSEARCH_DEFAULT_CLUSTER_NAME = "elasticsearch";

    /**
     * The Elasticsearch setting for client transport sniff
     */
    public static final String ELASTICSEARCH_SETTING_CLIENT_TRANSPORT_SNIFF = "client.transport.sniff";

    /**
     * The Elasticsearch setting for cluster name
     */
    public static final String ELASTICSEARCH_SETTING_CLUSTER_NAME = "cluster.name";

    /**
     * The Elasticsearch setting for path home
     */
    public static final String ELASTICSEARCH_SETTING_PATH_HOME = "path.home";

    /**
     * The Elasticsearch setting for path
     */
    public static final String ELASTICSEARCH_SETTING_PATH_HOME_PATH = ".";

    /**
     * Java Keystore type
     */
    public static final String JAVA_KEYSTORE_TYPE = "JKS";

    /**
     * Keystore key value
     */
    public static final String KEYSTORE_KEY = "KEYSTORE";

    /**
     * The network address cache ttl
     */
    public static final String NETWORK_ADDRESS_CACHE_TTL = "60";

    /**
     * Truststore key value
     */
    public static final String TRUSTSTORE_KEY = "TRUSTSTORE";


    /**
     * Method to retrieve a transport client used to connect to an search index. The transport client is cached.
     *
     * @return a transport client
     */
    @Cacheable(DaoSpringModuleConfig.TRANSPORT_CLIENT_CACHE_NAME)
    public TransportClient getTransportClient()
    {
        LOGGER.info("Updating the network address cash ttl value.");
        LOGGER.info("Network address cash ttl value setting before change, networkaddress.cache.ttl={}", Security.getProperty("networkaddress.cache.ttl"));
        Security.setProperty("networkaddress.cache.ttl", NETWORK_ADDRESS_CACHE_TTL);
        LOGGER.info("Network address cash ttl value setting after change, networkaddress.cache.ttl={}", Security.getProperty("networkaddress.cache.ttl"));

        LOGGER.info("Initializing transport client bean.");

        // Get the elasticsearch settings JSON string from the configuration
        String elasticSearchSettingsJSON = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SETTINGS_JSON);
        Integer port = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_DEFAULT_PORT, Integer.class);

        // Map the JSON object to the elastic search setting data transfer object
        ElasticsearchSettingsDto elasticsearchSettingsDto;
        try
        {
            elasticsearchSettingsDto = jsonHelper.unmarshallJsonToObject(ElasticsearchSettingsDto.class, elasticSearchSettingsJSON);
        }
        catch (IOException ioException)
        {
            // If there was an error creating the settings DTO, then setup a DTO with default values
            elasticsearchSettingsDto = new ElasticsearchSettingsDto();
            elasticsearchSettingsDto.setClientTransportSniff(true);
            elasticsearchSettingsDto.setElasticSearchCluster(ELASTICSEARCH_DEFAULT_CLUSTER_NAME);
            elasticsearchSettingsDto.setClientTransportAddresses(new ArrayList<>());
        }
        // Get the settings from the elasticsearch settings data transfer object
        String elasticSearchCluster = elasticsearchSettingsDto.getElasticSearchCluster();
        List<String> elasticSearchAddresses = elasticsearchSettingsDto.getClientTransportAddresses();
        boolean clientTransportStiff = elasticsearchSettingsDto.isClientTransportSniff();
        boolean isElasticsearchSearchGuardEnabled = Boolean.valueOf(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_ENABLED));
        LOGGER.info("isElasticsearchSearchGuardEnabled={}", isElasticsearchSearchGuardEnabled);

        // Build the Transport client with the settings
        Settings settings = Settings.builder().put(ELASTICSEARCH_SETTING_CLIENT_TRANSPORT_SNIFF, clientTransportStiff)
            .put(ELASTICSEARCH_SETTING_CLUSTER_NAME, elasticSearchCluster).build();

        LOGGER.info("Transport Client Settings:  clientTransportStiff={}, elasticSearchCluster={}, pathToKeystoreFile={}, pathToTruststoreFile={}",
            clientTransportStiff, elasticSearchCluster);

        TransportClient transportClient = preBuiltTransportClientFactory.getPreBuiltTransportClient(settings);

        // If search guard is enabled then setup the keystore and truststore
        if (isElasticsearchSearchGuardEnabled)
        {
            // Get the paths to the keystore and truststore files
            String pathToKeystoreFile = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_KEYSTORE_PATH);
            String pathToTruststoreFile = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_TRUSTSTORE_PATH);

            try
            {
                // Get the keystore and truststore passwords from Cred Stash
                Map<String, String> keystoreTruststorePasswordMap = getKeystoreAndTruststoreFromCredStash();

                // Retrieve the keystore password and truststore password from the keystore trustStore password map
                String keystorePassword = keystoreTruststorePasswordMap.get(KEYSTORE_KEY);
                String truststorePassword = keystoreTruststorePasswordMap.get(TRUSTSTORE_KEY);

                // Log the keystore and truststore information
                logKeystoreInformation(pathToKeystoreFile, keystorePassword);
                logKeystoreInformation(pathToTruststoreFile, truststorePassword);

                File keystoreFile = new File(pathToKeystoreFile);
                LOGGER.info("keystoreFile.name={}, keystoreFile.exists={}, keystoreFile.canRead={}", keystoreFile.getName(), keystoreFile.exists(),
                    keystoreFile.canRead());

                File truststoreFile = new File(pathToTruststoreFile);
                LOGGER.info("truststoreFile.name={}, truststoreFile.exists={}, truststoreFile.canRead={}", truststoreFile.getName(), truststoreFile.exists(),
                    truststoreFile.canRead());

                // Build the settings for the transport client
                settings = Settings.builder().put(ELASTICSEARCH_SETTING_CLIENT_TRANSPORT_SNIFF, clientTransportStiff)
                    .put(ELASTICSEARCH_SETTING_CLUSTER_NAME, elasticSearchCluster)
                    .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, keystoreFile)
                    .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, truststoreFile)
                    .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD, keystorePassword)
                    .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD, truststorePassword)
                    .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
                    .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME, false)
                    .put(ELASTICSEARCH_SETTING_PATH_HOME, ELASTICSEARCH_SETTING_PATH_HOME_PATH).build();

                LOGGER.info("Transport Client Settings:  clientTransportStiff={}, elasticSearchCluster={}, pathToKeystoreFile={}, pathToTruststoreFile={}",
                    clientTransportStiff, elasticSearchCluster, pathToKeystoreFile, pathToTruststoreFile);

                // Build the Transport client with the settings
                transportClient = preBuiltTransportClientFactory.getPreBuiltTransportClientWithSearchGuardPlugin(settings);
            }
            catch (CredStashGetCredentialFailedException credStashGetCredentialFailedException)
            {
                LOGGER.error("Failed to obtain credstash credentials.", credStashGetCredentialFailedException);
            }
        }

        // For each elastic search address in the elastic search address list
        for (String elasticSearchAddress : elasticSearchAddresses)
        {
            LOGGER.info("TransportClient add transport address elasticSearchAddress={}", elasticSearchAddress);
            // Add the address to the transport client
            try
            {
                transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticSearchAddress), port));
            }
            catch (UnknownHostException unknownHostException)
            {
                LOGGER.warn("Caught unknown host exception while attempting to add a transport address to the transport client.", unknownHostException);
            }
        }

        return transportClient;

    }

    /**
     * Private method to obtain the keystore and truststore passwords from cred stash. This method will attempt to obtain the credentials up to 3 times with a 5
     * second, 10 second, and 20 second back off
     *
     * @return a map containing the keystore and truststore passwords
     * @throws CredStashGetCredentialFailedException the cred stash get credential has failed
     */
    @Retryable(maxAttempts = 3, value = CredStashGetCredentialFailedException.class, backoff = @Backoff(delay = 5000, multiplier = 2))
    private Map<String, String> getKeystoreAndTruststoreFromCredStash() throws CredStashGetCredentialFailedException
    {
        // Get the credstash table name and credential names for the keystore and truststore
        String credstashEncryptionContext = configurationHelper.getProperty(ConfigurationValue.CREDSTASH_ENCRYPTION_CONTEXT);
        String credstashAwsRegion = configurationHelper.getProperty(ConfigurationValue.CREDSTASH_AWS_REGION_NAME);
        String credstashTableName = configurationHelper.getProperty(ConfigurationValue.CREDSTASH_TABLE_NAME);
        String keystoreCredentialName = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_KEYSTORE_CREDENTIAL_NAME);
        String truststoreCredentialName = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_TRUSTSTORE_CREDENTIAL_NAME);

        LOGGER.info("credstashTableName={}", credstashTableName);
        LOGGER.info("keystoreCredentialName={}", keystoreCredentialName);
        LOGGER.info("truststoreCredentialName={}", truststoreCredentialName);

        // Get the AWS client configuration.
        ClientConfiguration clientConfiguration = awsHelper.getClientConfiguration(awsHelper.getAwsParamsDto());

        // Get the keystore and truststore passwords from Credstash
        CredStash credstash = credStashFactory.getCredStash(credstashAwsRegion, credstashTableName, clientConfiguration);

        String keystorePassword = null;
        String truststorePassword = null;

        // Try to obtain the credentials from cred stash
        try
        {
            // Convert the JSON config file version of the encryption context to a Java Map class
            @SuppressWarnings("unchecked")
            Map<String, String> credstashEncryptionContextMap = jsonHelper.unmarshallJsonToObject(Map.class, credstashEncryptionContext);

            // Get the keystore and truststore passwords from credstash
            keystorePassword = credstash.getCredential(keystoreCredentialName, credstashEncryptionContextMap);
            truststorePassword = credstash.getCredential(truststoreCredentialName, credstashEncryptionContextMap);
        }
        catch (Exception exception)
        {
            LOGGER.error("Caught exception when attempting to get a credential value from CredStash", exception);
        }

        // If either the keystorePassword or truststorePassword values are empty and could not be obtained as credentials from cred stash,
        // then throw a new CredStashGetCredentialFailedException
        if (StringUtils.isEmpty(keystorePassword) || StringUtils.isEmpty(truststorePassword))
        {
            throw new CredStashGetCredentialFailedException("Failed to obtain the keystore or truststore credential from cred stash.");
        }

        // Return the keystore and truststore passwords in a map
        return ImmutableMap.<String, String>builder().
            put(KEYSTORE_KEY, keystorePassword).
            put(TRUSTSTORE_KEY, truststorePassword).
            build();
    }

    /**
     * Private method to log keystore file information.
     *
     * @param pathToKeystoreFile the path the the keystore file
     * @param keystorePassword the password that will open the keystore file
     */
    private void logKeystoreInformation(String pathToKeystoreFile, String keystorePassword)
    {
        try (final InputStream is = inputStreamFactory.getFileInputStream(pathToKeystoreFile))
        {
            final KeyStore keyStore = KeyStore.getInstance(JAVA_KEYSTORE_TYPE);

            keyStore.load(is, keystorePassword.toCharArray());

            Provider provider = keyStore.getProvider();

            LOGGER.info("Keystore file={}", pathToKeystoreFile);
            LOGGER.info("keystoreType={}", keyStore.getType());
            LOGGER.info("providerName={}", provider.getName());
            LOGGER.info("providerInfo={}", provider.getInfo());

            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements())
            {
                String alias = aliases.nextElement();
                LOGGER.info("certificate alias={}", alias);
                Certificate certificate = keyStore.getCertificate(alias);
                LOGGER.info("certificate publicKey={}", certificate.getPublicKey());
            }
        }
        catch (NoSuchAlgorithmException | CertificateException | IOException | KeyStoreException exception)
        {
            LOGGER.warn("Failed to log keystore information.", exception);
        }
    }
}
