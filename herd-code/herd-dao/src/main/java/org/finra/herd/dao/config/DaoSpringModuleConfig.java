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
import java.security.Security;
import java.security.cert.Certificate;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import com.amazonaws.retry.RetryPolicy.BackoffStrategy;
import com.floragunn.searchguard.ssl.SearchGuardSSLPlugin;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.google.common.collect.ImmutableMap;
import net.sf.ehcache.config.CacheConfiguration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.DatabaseConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CachingConfigurer;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.ehcache.EhCacheCacheManager;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.cache.interceptor.CacheResolver;
import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.cache.interceptor.SimpleCacheErrorHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.JpaVendorAdapter;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import org.finra.herd.core.ApplicationContextHolder;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.CacheKeyGenerator;
import org.finra.herd.dao.ReloadablePropertySource;
import org.finra.herd.dao.SimpleExponentialBackoffStrategy;
import org.finra.herd.dao.credstash.JCredStashWrapper;
import org.finra.herd.dao.exception.CredStashGetCredentialFailedException;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.ElasticsearchSettingsDto;
import org.finra.herd.model.jpa.ConfigurationEntity;

/**
 * DAO Spring module configuration.
 */
@Configuration
// Component scan all packages, but exclude the configuration ones since they are explicitly specified.
@ComponentScan(value = "org.finra.herd.dao",
    excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = "org\\.finra\\.herd\\.dao\\.config\\..*"))
@EnableTransactionManagement
@EnableCaching
public class DaoSpringModuleConfig implements CachingConfigurer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DaoSpringModuleConfig.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private JsonHelper jsonHelper;

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
     * The herd cache name.
     */
    public static final String HERD_CACHE_NAME = "herd_cache";

    /**
     * The herd data source bean name.
     */
    public static final String HERD_DATA_SOURCE_BEAN_NAME = "herdDataSource";

    /**
     * The herd transaction manager bean name.
     */
    public static final String HERD_TRANSACTION_MANAGER_BEAN_NAME = "herdTransactionManager";

    /**
     * The Hibernate HBM2DDL Auto param bean name.
     */
    public static final String HIBERNATE_HBM2DDL_AUTO_PARAM_BEAN_NAME = "hibernateHbm2DdlAutoParam";

    /**
     * Java Keystore type
     */
    public static final String JAVA_KEYSTORE_TYPE = "JKS";

    /**
     * Keystore key value
     */
    public static final String KEYSTORE_KEY = "KEYSTORE";

    /**
     * Model packages to scan by entity manager.
     */
    public static final String MODEL_PACKAGES_TO_SCAN = "org.finra.herd.model.jpa";

    /**
     * Truststore key value
     */
    public static final String TRUSTSTORE_KEY = "TRUSTSTORE";

    /**
     * The JPA entity manager factory.
     *
     * @return the entity manager factory.
     */
    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory()
    {
        // Create the entity manager factory against our data source.
        LocalContainerEntityManagerFactoryBean entityManagerFactory = new LocalContainerEntityManagerFactoryBean();
        entityManagerFactory.setDataSource(getHerdDataSource());

        // Auto-scan our model classes for persistent objects.
        entityManagerFactory.setPackagesToScan(MODEL_PACKAGES_TO_SCAN);

        // Set the JPA vendor adapter using a configured Spring bean.
        entityManagerFactory.setJpaVendorAdapter(getHibernateJpaVendorAdapter());

        // Set JPA additional properties.
        entityManagerFactory.setJpaProperties(jpaProperties());

        return entityManagerFactory;
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
        LOGGER.info("Updating the network address cash ttl value.");
        LOGGER.info("Network address cash ttl value setting before change, networkaddress.cache.ttl={}", Security.getProperty("networkaddress.cache.ttl"));
        Security.setProperty("networkaddress.cache.ttl", "60");
        LOGGER.info("Network address cash ttl value setting after change, networkaddress.cache.ttl={}", Security.getProperty("networkaddress.cache.ttl"));

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

    /**
     * Gets the Hibernate JPA vendor adapter needed by the entity manager.
     *
     * @return the Hibernate JPA vendor adapter.
     */
    private JpaVendorAdapter getHibernateJpaVendorAdapter()
    {
        HibernateJpaVendorAdapter hibernateJpaVendorAdapter = new HibernateJpaVendorAdapter();
        // Set the database type.
        String databaseType = configurationHelper.getProperty(ConfigurationValue.DATABASE_TYPE);
        if (StringUtils.isBlank(databaseType))
        {
            throw new IllegalStateException(
                String.format("No database type found. Ensure the \"%s\" configuration entry is configured.", ConfigurationValue.DATABASE_TYPE.getKey()));
        }
        Database database = Database.valueOf(databaseType);
        LOGGER.info("jpaTargetDatabase={}", database);
        hibernateJpaVendorAdapter.setDatabase(database);
        hibernateJpaVendorAdapter.setGenerateDdl(false);
        return hibernateJpaVendorAdapter;
    }

    /**
     * Gets the JPA properties that contain our Hibernate specific configuration parameters.
     *
     * @return the JPA properties.
     */
    private Properties jpaProperties()
    {
        // Create and return JPA properties.
        Properties properties = new Properties();
        // Set the Hibernate dialect.
        String hibernateDialect = configurationHelper.getProperty(ConfigurationValue.HIBERNATE_DIALECT);
        if (StringUtils.isBlank(hibernateDialect))
        {
            throw new IllegalStateException(String
                .format("No hibernate dialect found. Ensure the \"%s\" configuration entry is configured.", ConfigurationValue.HIBERNATE_DIALECT.getKey()));
        }
        properties.setProperty(ConfigurationValue.HIBERNATE_DIALECT.getKey(), hibernateDialect);
        LOGGER.info("hibernateDialect={}", properties.getProperty(ConfigurationValue.HIBERNATE_DIALECT.getKey()));
        properties.setProperty("hibernate.query.substitutions", "true='Y', false='N', yes='Y', no='N'");
        properties.setProperty("hibernate.cache.region.factory_class", "org.hibernate.cache.ehcache.EhCacheRegionFactory");
        properties.setProperty("hibernate.cache.use_query_cache", "true");
        properties.setProperty("hibernate.cache.use_second_level_cache", "true");
        // Set the "show sql" flag.
        properties.setProperty(ConfigurationValue.SHOW_SQL.getKey(), configurationHelper.getProperty(ConfigurationValue.SHOW_SQL));
        LOGGER.info("hibernateShowSql={}", properties.getProperty(ConfigurationValue.SHOW_SQL.getKey()));
        properties.setProperty("hibernate.archive.autodetection", "class, hbm");

        // Set the Hibernate HBM2DDL Auto param if it is configured. This is only needed in JUnits.
        String hibernateHbm2DdlAutoParam = getHibernateHbm2DdlAutoParam();
        if (StringUtils.isNotBlank(hibernateHbm2DdlAutoParam))
        {
            properties.setProperty("hibernate.hbm2ddl.auto", hibernateHbm2DdlAutoParam);
        }

        return properties;
    }

    /**
     * Our Spring JPA transaction manager that will manage the JPA transactions.
     *
     * @return the JPA transaction manager.
     */
    @Bean
    public JpaTransactionManager herdTransactionManager()
    {
        JpaTransactionManager transactionManager = new JpaTransactionManager();
        transactionManager.setDataSource(getHerdDataSource());
        transactionManager.setEntityManagerFactory(entityManagerFactory().getObject());
        return transactionManager;
    }

    /**
     * The database supplied property sources placeholder configurer that allows access to externalized properties from a database. This method also adds a new
     * property source that contains the database properties to the environment.
     *
     * @return the property sources placeholder configurer.
     */
    @Bean
    public static PropertySourcesPlaceholderConfigurer databasePropertySourcesPlaceholderConfigurer()
    {
        // Get the configurable environment and add a new property source to it that contains the database properties.
        // That way, the properties can be accessed via the environment or via an injected @Value annotation.
        // We are adding this property source last so other property sources (e.g. system properties, environment variables) can be used
        // to override the database properties.
        Environment environment = ApplicationContextHolder.getApplicationContext().getEnvironment();
        if (environment instanceof ConfigurableEnvironment)
        {
            ConfigurableEnvironment configurableEnvironment = (ConfigurableEnvironment) environment;
            ReloadablePropertySource reloadablePropertySource =
                new ReloadablePropertySource(ReloadablePropertySource.class.getName(), ConfigurationConverter.getProperties(getPropertyDatabaseConfiguration()),
                    getPropertyDatabaseConfiguration());
            configurableEnvironment.getPropertySources().addLast(reloadablePropertySource);
        }

        return new PropertySourcesPlaceholderConfigurer();
    }

    /**
     * Gets a database configuration that can be used to read database properties.
     *
     * @return the property database configuration.
     */
    private static DatabaseConfiguration getPropertyDatabaseConfiguration()
    {
        return new DatabaseConfiguration(getHerdDataSource(), ConfigurationEntity.TABLE_NAME, ConfigurationEntity.COLUMN_KEY, ConfigurationEntity.COLUMN_VALUE);
    }

    /**
     * Gets the data source bean from the application context statically. This is needed for static @Bean creation methods that won't have access to an
     * auto-wired data source.
     *
     * @return the data source.
     */
    public static DataSource getHerdDataSource()
    {
        return (DataSource) ApplicationContextHolder.getApplicationContext().getBean(HERD_DATA_SOURCE_BEAN_NAME);
    }

    /**
     * Gets the Hibernate HBM2DDL bean from the application context statically.
     *
     * @return the Hibernate HBM2DDL auto param.
     */
    public String getHibernateHbm2DdlAutoParam()
    {
        return (String) ApplicationContextHolder.getApplicationContext().getBean(HIBERNATE_HBM2DDL_AUTO_PARAM_BEAN_NAME);
    }

    /**
     * Gets an EH Cache manager.
     *
     * @return the EH Cache manager.
     */
    @Bean(destroyMethod = "shutdown")
    public net.sf.ehcache.CacheManager ehCacheManager()
    {
        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setName(HERD_CACHE_NAME);
        cacheConfiguration.setTimeToLiveSeconds(configurationHelper.getProperty(ConfigurationValue.HERD_CACHE_TIME_TO_LIVE_SECONDS, Long.class));
        cacheConfiguration.setTimeToIdleSeconds(configurationHelper.getProperty(ConfigurationValue.HERD_CACHE_TIME_TO_IDLE_SECONDS, Long.class));
        cacheConfiguration.setMaxElementsInMemory(configurationHelper.getProperty(ConfigurationValue.HERD_CACHE_MAX_ELEMENTS_IN_MEMORY, Integer.class));
        cacheConfiguration.setMemoryStoreEvictionPolicy(configurationHelper.getProperty(ConfigurationValue.HERD_CACHE_MEMORY_STORE_EVICTION_POLICY));

        net.sf.ehcache.config.Configuration config = new net.sf.ehcache.config.Configuration();
        config.addCache(cacheConfiguration);

        return net.sf.ehcache.CacheManager.create(config);
    }

    @Bean
    @Override
    public CacheManager cacheManager()
    {
        return new EhCacheCacheManager(ehCacheManager());
    }

    @Bean
    @Override
    public KeyGenerator keyGenerator()
    {
        return new CacheKeyGenerator();
    }

    /*
     * No need to have a cache resolver, this is optional as we are defining cacheManager.
     */
    @Bean
    @Override
    public CacheResolver cacheResolver()
    {
        return null;
    }

    @Bean
    @Override
    public CacheErrorHandler errorHandler()
    {
        return new SimpleCacheErrorHandler();
    }

    @Bean
    public BackoffStrategy backoffStrategy()
    {
        return new SimpleExponentialBackoffStrategy();
    }

    /**
     * Gets an LDAP context source.
     *
     * @return the LDAP context source
     */
    @Bean
    public LdapContextSource contextSource()
    {
        String ldapUrl = configurationHelper.getProperty(ConfigurationValue.LDAP_URL);
        String ldapBase = configurationHelper.getProperty(ConfigurationValue.LDAP_BASE);
        String ldapUserDn = configurationHelper.getProperty(ConfigurationValue.LDAP_USER_DN);

        LOGGER.info("Creating LDAP context source using the following parameters: {}=\"{}\" {}=\"{}\" {}=\"{}\" ...", ConfigurationValue.LDAP_URL.getKey(),
            ldapUrl, ConfigurationValue.LDAP_BASE.getKey(), ldapBase, ConfigurationValue.LDAP_USER_DN.getKey(), ldapUserDn);

        LdapContextSource contextSource = new LdapContextSource();
        contextSource.setUrl(ldapUrl);
        contextSource.setBase(ldapBase);
        contextSource.setUserDn(ldapUserDn);
        contextSource.setPassword(configurationHelper.getProperty(ConfigurationValue.LDAP_PASSWORD));
        return contextSource;
    }

    /**
     * Gets an LDAP template.
     *
     * @return the LDAP template
     */
    @Bean
    public LdapTemplate ldapTemplate()
    {
        return new LdapTemplate(contextSource());
    }
}
