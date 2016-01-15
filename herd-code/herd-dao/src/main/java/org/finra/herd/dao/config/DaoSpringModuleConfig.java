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

import java.util.Properties;

import javax.sql.DataSource;

import net.sf.ehcache.config.CacheConfiguration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.DatabaseConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
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
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.JpaVendorAdapter;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import org.finra.herd.core.ApplicationContextHolder;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.CacheKeyGenerator;
import org.finra.herd.dao.ReloadablePropertySource;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.ConfigurationEntity;

/**
 * DAO Spring module configuration.
 */
@Configuration
// Component scan all packages, but exclude the configuration ones since they are explicitly specified.
@ComponentScan(value = "org.finra.herd.dao",
    excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = "org\\.finra\\.herd\\.dao\\.config\\..*"))
@Import(DaoAopSpringModuleConfig.class)
@EnableTransactionManagement
@EnableCaching
public class DaoSpringModuleConfig implements CachingConfigurer
{
    private static final Logger LOGGER = Logger.getLogger(DaoSpringModuleConfig.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * The herd data source bean name.
     */
    public static final String HERD_DATA_SOURCE_BEAN_NAME = "herdDataSource";

    /**
     * The Hibernate HBM2DDL Auto param bean name.
     */
    public static final String HIBERNATE_HBM2DDL_AUTO_PARAM_BEAN_NAME = "hibernateHbm2DdlAutoParam";

    /**
     * The herd transaction manager bean name.
     */
    public static final String HERD_TRANSACTION_MANAGER_BEAN_NAME = "herdTransactionManager";

    /**
     * Model packages to scan by entity manager.
     */
    public static final String MODEL_PACKAGES_TO_SCAN = "org.finra.herd.model.jpa";

    /**
     * The herd cache name.
     */
    public static final String HERD_CACHE_NAME = "herd_cache";

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
        LOGGER.info("JPA Target Database: " + database);
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
        LOGGER.info("Hibernate Dialect: " + properties.getProperty(ConfigurationValue.HIBERNATE_DIALECT.getKey()));
        properties.setProperty("hibernate.query.substitutions", "true='Y', false='N', yes='Y', no='N'");
        properties.setProperty("hibernate.cache.region.factory_class", "org.hibernate.cache.ehcache.EhCacheRegionFactory");
        properties.setProperty("hibernate.cache.use_query_cache", "true");
        properties.setProperty("hibernate.cache.use_second_level_cache", "true");
        // Set the "show sql" flag.
        properties.setProperty(ConfigurationValue.SHOW_SQL.getKey(), configurationHelper.getProperty(ConfigurationValue.SHOW_SQL));
        LOGGER.info("Show SQL: " + properties.getProperty(ConfigurationValue.SHOW_SQL.getKey()));
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
}
