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

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.lookup.JndiDataSourceLookup;

import org.finra.herd.core.ApplicationContextHolder;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.Ec2Operations;
import org.finra.herd.dao.EmrOperations;
import org.finra.herd.dao.HttpClientOperations;
import org.finra.herd.dao.JdbcOperations;
import org.finra.herd.dao.KmsOperations;
import org.finra.herd.dao.LdapOperations;
import org.finra.herd.dao.S3Operations;
import org.finra.herd.dao.SnsOperations;
import org.finra.herd.dao.SqsOperations;
import org.finra.herd.dao.StsOperations;
import org.finra.herd.dao.UrlOperations;
import org.finra.herd.dao.impl.Ec2OperationsImpl;
import org.finra.herd.dao.impl.EmrOperationsImpl;
import org.finra.herd.dao.impl.HttpClientOperationsImpl;
import org.finra.herd.dao.impl.JdbcOperationsImpl;
import org.finra.herd.dao.impl.KmsOperationsImpl;
import org.finra.herd.dao.impl.LdapOperationsImpl;
import org.finra.herd.dao.impl.S3OperationsImpl;
import org.finra.herd.dao.impl.SnsOperationsImpl;
import org.finra.herd.dao.impl.SqsOperationsImpl;
import org.finra.herd.dao.impl.StsOperationsImpl;
import org.finra.herd.dao.impl.UrlOperationsImpl;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * DAO environment specific Spring module configuration.
 */
@Configuration
public class DaoEnvSpringModuleConfig
{
    /**
     * The data source for the application.
     *
     * @return the data source.
     */
    @Bean
    public static DataSource herdDataSource()
    {
        // Access the environment using the application context holder since we're in a static method that doesn't have access to the environment in any
        // other way.
        Environment environment = ApplicationContextHolder.getApplicationContext().getEnvironment();

        // Get the configuration property for the data source JNDI name.
        String dataSourceJndiName = ConfigurationHelper.getProperty(ConfigurationValue.HERD_DATA_SOURCE_JNDI_NAME, environment);

        // Return a new JNDI data source.
        return new JndiDataSourceLookup().getDataSource(dataSourceJndiName);
    }

    /**
     * Gets the Hibernate HBM2DDL Auto param. This method returns null which is used to not automatically create the schema.
     *
     * @return the Hibernate HBM2DDL Auto param.
     */
    @Bean
    public static String hibernateHbm2DdlAutoParam()
    {
        return null;
    }

    @Bean
    public S3Operations s3Operations()
    {
        return new S3OperationsImpl();
    }

    @Bean
    public EmrOperations emrOperations()
    {
        return new EmrOperationsImpl();
    }

    @Bean
    public Ec2Operations ec2Operations()
    {
        return new Ec2OperationsImpl();
    }

    @Bean
    public SqsOperations sqsOperations()
    {
        return new SqsOperationsImpl();
    }

    @Bean
    public SnsOperations snsOperations()
    {
        return new SnsOperationsImpl();
    }

    @Bean
    public StsOperations stsOperations()
    {
        return new StsOperationsImpl();
    }

    @Bean
    public JdbcOperations jdbcOperations()
    {
        return new JdbcOperationsImpl();
    }

    @Bean
    public LdapOperations ldapOperations()
    {
        return new LdapOperationsImpl();
    }

    @Bean
    public KmsOperations kmsOperations()
    {
        return new KmsOperationsImpl();
    }

    @Bean
    public HttpClientOperations httpClientOperations()
    {
        return new HttpClientOperationsImpl();
    }

    @Bean
    public UrlOperations urlOperations()
    {
        return new UrlOperationsImpl();
    }
}
