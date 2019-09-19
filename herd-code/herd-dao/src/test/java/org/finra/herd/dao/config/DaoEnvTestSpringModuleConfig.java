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

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import org.finra.herd.dao.Ec2Operations;
import org.finra.herd.dao.EmrDao;
import org.finra.herd.dao.EmrDaoImplTest;
import org.finra.herd.dao.EmrOperations;
import org.finra.herd.dao.HttpClientOperations;
import org.finra.herd.dao.JdbcOperations;
import org.finra.herd.dao.KmsOperations;
import org.finra.herd.dao.LdapOperations;
import org.finra.herd.dao.Log4jOverridableConfigurer;
import org.finra.herd.dao.S3Operations;
import org.finra.herd.dao.SesOperations;
import org.finra.herd.dao.SnsOperations;
import org.finra.herd.dao.SqsOperations;
import org.finra.herd.dao.StsOperations;
import org.finra.herd.dao.UrlOperations;
import org.finra.herd.dao.impl.MockEc2OperationsImpl;
import org.finra.herd.dao.impl.MockEmrOperationsImpl;
import org.finra.herd.dao.impl.MockHttpClientOperationsImpl;
import org.finra.herd.dao.impl.MockJdbcOperations;
import org.finra.herd.dao.impl.MockKmsOperationsImpl;
import org.finra.herd.dao.impl.MockLdapOperations;
import org.finra.herd.dao.impl.MockS3OperationsImpl;
import org.finra.herd.dao.impl.MockSesOperationsImpl;
import org.finra.herd.dao.impl.MockSnsOperationsImpl;
import org.finra.herd.dao.impl.MockSqsOperationsImpl;
import org.finra.herd.dao.impl.MockStsOperationsImpl;
import org.finra.herd.dao.impl.MockUrlOperationsImpl;

/**
 * DAO environment test specific Spring module configuration.
 */
@Configuration
public class DaoEnvTestSpringModuleConfig
{
    /**
     * The Log4J configuration location for the JUnit tests.
     */
    public static final String TEST_LOG4J_CONFIG_RESOURCE_LOCATION = "classpath:herd-log4j-test.xml";

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
    public SesOperations sesOperations()
    {
        return new MockSesOperationsImpl();
    }

    @Bean
    public SqsOperations sqsOperations()
    {
        return new MockSqsOperationsImpl();
    }

    @Bean
    public SnsOperations snsOperations()
    {
        return new MockSnsOperationsImpl();
    }

    @Bean
    public StsOperations stsOperations()
    {
        return new MockStsOperationsImpl();
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

    @Bean
    public UrlOperations urlOperations()
    {
        return new MockUrlOperationsImpl();
    }
}
