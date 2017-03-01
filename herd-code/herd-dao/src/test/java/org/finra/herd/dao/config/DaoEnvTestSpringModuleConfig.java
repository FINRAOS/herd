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
package org.finra.herd.dao.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.Provider;
import java.security.cert.Certificate;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.Ec2Operations;
import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.EmrDaoImplTest;
import org.finra.herd.dao.EmrOperations;
import org.finra.herd.dao.HttpClientOperations;
import org.finra.herd.dao.JdbcOperations;
import org.finra.herd.dao.KmsOperations;
import org.finra.herd.dao.LdapOperations;
import org.finra.herd.dao.Log4jOverridableConfigurer;
import org.finra.herd.dao.OozieOperations;
import org.finra.herd.dao.S3Operations;
import org.finra.herd.dao.SqsOperations;
import org.finra.herd.dao.StsOperations;
import org.finra.herd.dao.credstash.JCredStashWrapper;
import org.finra.herd.dao.exception.CredStashGetCredentialFailedException;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.dao.impl.MockEc2OperationsImpl;
import org.finra.herd.dao.impl.MockEmrOperationsImpl;
import org.finra.herd.dao.impl.MockHttpClientOperationsImpl;
import org.finra.herd.dao.impl.MockJdbcOperations;
import org.finra.herd.dao.impl.MockKmsOperationsImpl;
import org.finra.herd.dao.impl.MockLdapOperations;
import org.finra.herd.dao.impl.MockOozieOperationsImpl;
import org.finra.herd.dao.impl.MockS3OperationsImpl;
import org.finra.herd.dao.impl.MockSqsOperationsImpl;
import org.finra.herd.dao.impl.MockStsOperationsImpl;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.ElasticsearchSettingsDto;

/**
 * DAO environment test specific Spring module configuration.
 */
@Configuration
public class DaoEnvTestSpringModuleConfig
{
    /**
     * Logger for the ServiceSpringModuleConfig class
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DaoEnvTestSpringModuleConfig.class);

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
     * The Log4J configuration location for the JUnit tests.
     */
    public static final String TEST_LOG4J_CONFIG_RESOURCE_LOCATION = "classpath:herd-log4j-test.xml";

    /**
     * Truststore key value
     */
    public static final String TRUSTSTORE_KEY = "TRUSTSTORE";

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private JsonHelper jsonHelper;

    /**
     * This is a data source initializer which is used to make changes to the auto-created schema based on JPA annotations and to insert reference data. This
     * bean is an InitializingBean which means it will automatically get invoked when the Spring test context creates all its beans. This approach will work for
     * making changes to the auto-created schema which got created based on other DAO beans having been created.
     *
     * @return the data source initializer.
     */
    @Bean
    public static DataSourceInitializer dataSourceInitializer()
    {
        ResourceDatabasePopulator resourceDatabasePopulator = new ResourceDatabasePopulator();
        resourceDatabasePopulator.addScript(new ClassPathResource("alterJpaTablesAndInsertReferenceData.sql"));

        DataSourceInitializer dataSourceInitializer = new DataSourceInitializer();
        dataSourceInitializer.setDataSource(herdDataSource());
        dataSourceInitializer.setDatabasePopulator(resourceDatabasePopulator);
        return dataSourceInitializer;
    }

    /**
     * Get a new herd data source based on an in-memory HSQLDB database. This data source is used for loading the configuration table as an environment property
     * source as well as for the JPA entity manager. It will therefore create/re-create the configuration table which is required for the former. It also
     * inserts required values for both scenarios.
     *
     * @return the test herd data source.
     */
    @Bean
    public static DataSource herdDataSource()
    {
        // Create and return a data source that can connect directly to a JDBC URL.
        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName(org.h2.Driver.class.getName());
        basicDataSource.setUsername("");
        basicDataSource.setPassword("");
        basicDataSource.setUrl("jdbc:h2:mem:herdTestDb");

        // Create and populate the configuration table.
        // This is needed for all data source method calls since it is used to create the environment property source which happens before
        // JPA and other non-static beans are initialized.
        ResourceDatabasePopulator resourceDatabasePopulator = new ResourceDatabasePopulator();
        resourceDatabasePopulator.addScript(new ClassPathResource("createConfigurationTableAndData.sql"));
        DatabasePopulatorUtils.execute(resourceDatabasePopulator, basicDataSource); // This is what the DataSourceInitializer does.

        return basicDataSource;
    }

    /**
     * Gets the Hibernate HBM2DDL Auto param. This method returns "create" which is used to create the schema from the JPA annotations. Valid values are:
     * <p/>
     * <pre>
     * validate: validate that the schema matches, make no changes to the schema of the database, you probably want this for production.
     * update: update the schema to reflect the entities being persisted
     * create: creates the schema necessary for your entities, destroying any previous data.
     * create-drop: create the schema as in create above, but also drop the schema at the end of the session. This is great in early development or for
     * testing.
     * </pre>
     *
     * @return the Hibernate HBM2DDL Auto param.
     */
    @Bean
    public static String hibernateHbm2DdlAutoParam()
    {
        return "create";
    }

    /**
     * The Log4J configuration used by JUnits. It is defined in the DAO tier so all tiers that extend it can take advantage of it.
     * <p/>
     * IMPORTANT: Ensure this method is static since the returned Log4jOverridableConfigurer is a bean factory post processor (BFPP). If it weren't static,
     * autowiring and injection on this @Configuration class won't work due to lifecycle issues. See "Bootstrapping" comment in @Bean annotation for more
     * details.
     *
     * @return the Log4J overridable configurer.
     */
    @Bean
    public static Log4jOverridableConfigurer log4jConfigurer()
    {
        Log4jOverridableConfigurer log4jConfigurer = new Log4jOverridableConfigurer();
        log4jConfigurer.setDefaultResourceLocation(TEST_LOG4J_CONFIG_RESOURCE_LOCATION);
        log4jConfigurer.setOverrideResourceLocation("non_existent_override_location");
        return log4jConfigurer;
    }

    @Bean
    public S3Operations s3Operations()
    {
        return new MockS3OperationsImpl();
    }

    @Bean
    public EmrOperations emrOperations()
    {
        return new MockEmrOperationsImpl();
    }

    @Bean
    public Ec2Operations ec2Operations()
    {
        return new MockEc2OperationsImpl();
    }

    @Bean
    public SqsOperations sqsOperations()
    {
        return new MockSqsOperationsImpl();
    }

    @Bean
    public StsOperations stsOperations()
    {
        return new MockStsOperationsImpl();
    }

    @Bean
    public OozieOperations oozieOperations()
    {
        return new MockOozieOperationsImpl();
    }

    @Bean
    public JdbcOperations jdbcOperations()
    {
        return new MockJdbcOperations();
    }

    @Bean
    public LdapOperations ldapOperations()
    {
        return new MockLdapOperations();
    }

    @Bean
    public KmsOperations kmsOperations()
    {
        return new MockKmsOperationsImpl();
    }

    @Bean
    public HttpClientOperations httpClientOperations()
    {
        return new MockHttpClientOperationsImpl();
    }

    @Bean
    public EmrDao emrDao()
    {
        return new EmrDaoImplTest();
    }

    /**
     * Returns an elasticsearch transport client.
     *
     * @return TransportClient for the elasticsearch client is returned.
     * @throws Exception is thrown if host can not be found, or if settings object can not be mapped.
     */
    @Bean
    public TransportClient transportClient() throws Exception
    {
        LOGGER.info("Initializing transport client bean.");

        // Get the elasticsearch settings JSON string from the configuration
        String elasticSearchSettingsJSON = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SETTINGS_JSON);
        Integer port = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_DEFAULT_PORT, Integer.class);

        // Map the JSON object to the elastic search setting data transfer object
        ElasticsearchSettingsDto elasticsearchSettingsDto = jsonHelper.unmarshallJsonToObject(ElasticsearchSettingsDto.class, elasticSearchSettingsJSON);

        // Get the settings from the elasticsearch settings data transfer object
        String elasticSearchCluster = elasticsearchSettingsDto.getElasticSearchCluster();
        List<String> elasticSearchAddresses = elasticsearchSettingsDto.getClientTransportAddresses();
        boolean clientTransportStiff = elasticsearchSettingsDto.isClientTransportSniff();
        boolean isElasticsearchSearchGuardEnabled = Boolean.valueOf(configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_ENABLED));
        LOGGER.info("isElasticsearchSearchGuardEnabled={}", isElasticsearchSearchGuardEnabled);

        // Build the Transport client with the settings
        Settings settings;
        TransportClient transportClient;

        // If search guard is enabled then setup the keystore and truststore
        if (isElasticsearchSearchGuardEnabled)
        {
            // Get the paths to the keystore and truststore files
            String pathToKeystoreFile = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_KEYSTORE_PATH);
            String pathToTruststoreFile = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_SEARCH_GUARD_TRUSTSTORE_PATH);

            // Get the keystore and truststore passwords from Cred Stash
            Map<String, String> keystoreTruststorePasswordMap = getKeystoreAndTruststoreFromCredStash();

            // Retreive the keystore password and truststore password from the keystore trustStore password map
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
                .put(ELASTICSEARCH_SETTING_CLUSTER_NAME, elasticSearchCluster).put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH, keystoreFile)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH, truststoreFile)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD, keystorePassword)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD, truststorePassword)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION, false)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME, false)
                .put(ELASTICSEARCH_SETTING_PATH_HOME, ELASTICSEARCH_SETTING_PATH_HOME_PATH).build();

            LOGGER.info("Transport Client Settings:  clientTransportStiff={}, elasticSearchCluster={}, pathToKeystoreFile={}, pathToTruststoreFile={}",
                clientTransportStiff, elasticSearchCluster, pathToKeystoreFile, pathToTruststoreFile);

            // Build the Transport client with the settings
            transportClient = new PreBuiltTransportClient(settings, SearchGuardSSLPlugin.class);
        }
        else
        {
            // Build the settings for the transport client
            settings = Settings.builder().put(ELASTICSEARCH_SETTING_CLIENT_TRANSPORT_SNIFF, clientTransportStiff)
                .put(ELASTICSEARCH_SETTING_CLUSTER_NAME, elasticSearchCluster).build();

            LOGGER.info("Transport Client Settings:  clientTransportStiff={}, elasticSearchCluster={}, pathToKeystoreFile={}, pathToTruststoreFile={}",
                clientTransportStiff, elasticSearchCluster);

            // Build the Transport client with the settings
            transportClient = new PreBuiltTransportClient(settings);
        }

        // For each elastic search address in the elastic search address list
        for (String elasticSearchAddress : elasticSearchAddresses)
        {
            LOGGER.info("TransportClient add transport address elasticSearchAddress={}", elasticSearchAddress);
            // Add the address to the transport client
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticSearchAddress), port));
        }

        return transportClient;
    }

    /**
     * Private method to obtain the keystore and truststore passwords from cred stash. This method will attempt to obtain the credentials up to 3 times with a 5
     * second, 10 second, and 20 second back off
     *
     * @return a map containing the keystore and truststore passwords
     * @throws CredStashGetCredentialFailedException
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

        // Get the keystore and truststore passwords from Credstash
        JCredStashWrapper credstash = new JCredStashWrapper(credstashAwsRegion, credstashTableName);

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
     *
     * @throws Exception a potential exception when getting the keystore
     */
    private void logKeystoreInformation(String pathToKeystoreFile, String keystorePassword) throws Exception
    {

        final KeyStore keyStore = KeyStore.getInstance(JAVA_KEYSTORE_TYPE);

        try (final InputStream is = new FileInputStream(pathToKeystoreFile))
        {
            keyStore.load(is, keystorePassword.toCharArray());
        }

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
}
