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
package org.finra.dm.dao.config;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.lookup.JndiDataSourceLookup;

import org.finra.dm.dao.Ec2Operations;
import org.finra.dm.dao.EmrOperations;
import org.finra.dm.dao.HttpClientOperations;
import org.finra.dm.dao.JdbcOperations;
import org.finra.dm.dao.KmsOperations;
import org.finra.dm.dao.OozieOperations;
import org.finra.dm.dao.S3Operations;
import org.finra.dm.dao.SqsOperations;
import org.finra.dm.dao.StsOperations;
import org.finra.dm.dao.impl.Ec2OperationsImpl;
import org.finra.dm.dao.impl.EmrOperationsImpl;
import org.finra.dm.dao.impl.HttpClientOperationsImpl;
import org.finra.dm.dao.impl.JdbcOperationsImpl;
import org.finra.dm.dao.impl.KmsOperationsImpl;
import org.finra.dm.dao.impl.OozieOperationsImpl;
import org.finra.dm.dao.impl.S3OperationsImpl;
import org.finra.dm.dao.impl.SqsOperationsImpl;
import org.finra.dm.dao.impl.StsOperationsImpl;

/**
 * DAO environment specific Spring module configuration.
 */
@Configuration
public class DaoEnvSpringModuleConfig
{
    /**
     * The data source JNDI name.
     */
    public static final String DM_DATA_SOURCE_JNDI_NAME = "java:comp/env/jdbc/DMDB";

    /**
     * The data source for the application.
     *
     * @return the data source.
     */
    @Bean
    public static DataSource dmDataSource()
    {
        return new JndiDataSourceLookup().getDataSource(DM_DATA_SOURCE_JNDI_NAME);
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
    public StsOperations stsOperations()
    {
        return new StsOperationsImpl();
    }

    @Bean
    public OozieOperations oozieOperations()
    {
        return new OozieOperationsImpl();
    }

    @Bean
    public JdbcOperations jdbcOperations()
    {
        return new JdbcOperationsImpl();
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
}
