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
package org.finra.herd.dao.credstash;

import java.util.Map;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kms.AWSKMSClient;
import com.jessecoyle.CredStashBouncyCastleCrypto;
import com.jessecoyle.JCredStash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A wrapper class for the JCredStash library
 */
public class JCredStashWrapper implements CredStash
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JCredStashWrapper.class);

    private JCredStash credstash;

    /**
     * Constructor for the JCredStashWrapper
     *
     * @param region the aws region location of the KMS Client
     * @param tableName name of the credentials table
     */
    public JCredStashWrapper(String region, String tableName)
    {
        ClientConfiguration clientConf = defaultClientConfiguration(new EnvConfig());
        AWSCredentialsProvider provider = new DefaultAWSCredentialsProviderChain();
        AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(provider, clientConf).withRegion(Regions.fromName(region));
        AWSKMSClient kms = new AWSKMSClient(provider, clientConf).withRegion(Regions.fromName(region));
        credstash = new JCredStash(tableName, ddb, kms, new CredStashBouncyCastleCrypto());
    }

    /**
     * Private method to get the client configuration
     *
     * @param envConfig the configuration from the environment
     *
     * @return the client configuration
     */
    private ClientConfiguration defaultClientConfiguration(EnvConfig envConfig)
    {
        ClientConfiguration clientConfiguration = new ClientConfiguration();

        if (envConfig.hasProxyEnv())
        {
            clientConfiguration.setProxyHost(envConfig.getProxy());
            clientConfiguration.setProxyPort(Integer.parseInt(envConfig.getPort()));
        }

        return clientConfiguration;
    }

    /**
     * @param name Base name of the credential to retrieve
     * @param context key value map
     *
     * @return The plaintext contents of the credential (most recent version)
     * @throws Exception the runtime exception if the credential is not found
     */
    public String getCredential(String name, Map<String, String> context) throws Exception
    {
        String credential = null;

        try
        {
            credential = credstash.getSecret(name, context);
            LOGGER.info("Retrieved contents of " + name);
        }
        catch (RuntimeException e)
        {
            // Credential not found
            LOGGER.error("Credential " + name + " not found. ", e);
        }

        return credential;
    }
}