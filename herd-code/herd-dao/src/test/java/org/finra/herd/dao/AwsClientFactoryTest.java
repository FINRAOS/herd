package org.finra.herd.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQS;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.dto.AwsParamsDto;

public class AwsClientFactoryTest extends AbstractDaoTest
{
    @Autowired
    private AwsClientFactory awsClientFactory;

    @Autowired
    private CacheManager cacheManager;

    @Test
    public void testGetAmazonSNSClient()
    {
        assertNotNull(
            awsClientFactory.getAmazonSNSClient(new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT)));
    }

    @Test
    public void testGetAmazonSNSClientCacheHitMiss()
    {
        // Create an AWS parameters DTO that contains proxy information.
        AwsParamsDto awsParamsDto = new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT);

        // Get an Amazon SNS client.
        AmazonSNS amazonSNS = awsClientFactory.getAmazonSNSClient(awsParamsDto);

        // Confirm a cache hit.
        assertEquals(amazonSNS,
            awsClientFactory.getAmazonSNSClient(new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT)));

        // Confirm a cache miss due to http proxy information.
        assertNotEquals(amazonSNS, awsClientFactory
            .getAmazonSNSClient(new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST_2, HTTP_PROXY_PORT_2)));

        // Clear the cache.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        // Confirm a cache miss due to cleared cache.
        assertNotEquals(amazonSNS, awsClientFactory.getAmazonSNSClient(awsParamsDto));
    }

    @Test
    public void testGetAmazonSQSClient()
    {
        assertNotNull(
            awsClientFactory.getAmazonSQSClient(new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT)));
    }

    @Test
    public void testGetAmazonSQSClientCacheHitMiss()
    {
        // Create an AWS parameters DTO that contains proxy information.
        AwsParamsDto awsParamsDto = new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT);

        // Get an Amazon SQS client.
        AmazonSQS amazonSQS = awsClientFactory.getAmazonSQSClient(awsParamsDto);

        // Confirm a cache hit.
        assertEquals(amazonSQS,
            awsClientFactory.getAmazonSQSClient(new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT)));

        // Confirm a cache miss due to http proxy information.
        assertNotEquals(amazonSQS, awsClientFactory
            .getAmazonSQSClient(new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST_2, HTTP_PROXY_PORT_2)));

        // Clear the cache.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        // Confirm a cache miss due to cleared cache.
        assertNotEquals(amazonSQS, awsClientFactory.getAmazonSQSClient(awsParamsDto));
    }

    @Test
    public void testGetEc2Client()
    {
        assertNotNull(awsClientFactory.getEc2Client(
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT)));
    }

    @Test
    public void testGetEc2ClientCacheHitMiss()
    {
        // Create an AWS parameters DTO that contains both AWS credentials and proxy information.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT);

        // Get an Amazon EC2 client.
        AmazonEC2Client amazonEC2Client = awsClientFactory.getEc2Client(awsParamsDto);

        // Confirm a cache hit.
        assertEquals(amazonEC2Client, awsClientFactory.getEc2Client(
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT)));

        // Confirm a cache miss due to AWS credentials.
        assertNotEquals(amazonEC2Client, awsClientFactory.getEc2Client(
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY_2, AWS_ASSUMED_ROLE_SECRET_KEY_2, AWS_ASSUMED_ROLE_SESSION_TOKEN_2, HTTP_PROXY_HOST,
                HTTP_PROXY_PORT)));

        // Confirm a cache miss due to http proxy information.
        assertNotEquals(amazonEC2Client, awsClientFactory.getEc2Client(
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST_2, HTTP_PROXY_PORT_2)));

        // Clear the cache.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        // Confirm a cache miss due to cleared cache.
        assertNotEquals(amazonEC2Client, awsClientFactory.getEc2Client(awsParamsDto));
    }

    @Test
    public void testGetEc2ClientNoAwsCredentials()
    {
        assertNotNull(
            awsClientFactory.getEc2Client(new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT)));
    }

    @Test
    public void testGetEmrClient()
    {
        assertNotNull(awsClientFactory.getEmrClient(
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT)));
    }

    @Test
    public void testGetEmrClientCacheHitMiss()
    {
        // Create an AWS parameters DTO that contains both AWS credentials and proxy information.
        AwsParamsDto awsParamsDto =
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT);

        // Get an Amazon EMR client.
        AmazonElasticMapReduceClient amazonElasticMapReduceClient = awsClientFactory.getEmrClient(awsParamsDto);

        // Confirm a cache hit.
        assertEquals(amazonElasticMapReduceClient, awsClientFactory.getEmrClient(
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT)));

        // Confirm a cache miss due to AWS credentials.
        assertNotEquals(amazonElasticMapReduceClient, awsClientFactory.getEmrClient(
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY_2, AWS_ASSUMED_ROLE_SECRET_KEY_2, AWS_ASSUMED_ROLE_SESSION_TOKEN_2, HTTP_PROXY_HOST,
                HTTP_PROXY_PORT)));

        // Confirm a cache miss due to http proxy information.
        assertNotEquals(amazonElasticMapReduceClient, awsClientFactory.getEmrClient(
            new AwsParamsDto(AWS_ASSUMED_ROLE_ACCESS_KEY, AWS_ASSUMED_ROLE_SECRET_KEY, AWS_ASSUMED_ROLE_SESSION_TOKEN, HTTP_PROXY_HOST_2, HTTP_PROXY_PORT_2)));

        // Clear the cache.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();

        // Confirm a cache miss due to cleared cache.
        assertNotEquals(amazonElasticMapReduceClient, awsClientFactory.getEmrClient(awsParamsDto));
    }

    @Test
    public void testGetEmrClientNoAwsCredentials()
    {
        assertNotNull(
            awsClientFactory.getEmrClient(new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, HTTP_PROXY_HOST, HTTP_PROXY_PORT)));
    }
}
