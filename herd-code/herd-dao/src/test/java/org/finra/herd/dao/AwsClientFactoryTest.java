package org.finra.herd.dao;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.dto.AwsParamsDto;

public class AwsClientFactoryTest extends AbstractDaoTest
{
    @Autowired
    private CacheManager cacheManager;

    
    @Autowired
    private AwsClientFactory awsClientFactory;
    
    @Test
    public void getEmrClientCachingHit() throws Exception
    {
        String httpProxyHost = "";
        Integer httpProxyPort = 1234;

        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setHttpProxyHost(httpProxyHost);
        awsParamsDto.setHttpProxyPort(httpProxyPort);
       
        AmazonElasticMapReduceClient amazonElasticMapReduceClient = awsClientFactory.getEmrClient(awsParamsDto);
        
        AwsParamsDto awsParamsDto2 = new AwsParamsDto();
        awsParamsDto2.setHttpProxyHost(httpProxyHost);
        awsParamsDto2.setHttpProxyPort(httpProxyPort);
        
        AmazonElasticMapReduceClient amazonElasticMapReduceClient2 = awsClientFactory.getEmrClient(awsParamsDto2);
        
        assertTrue(amazonElasticMapReduceClient == amazonElasticMapReduceClient2);
    }
    
    @Test
    public void getEmrClientCachingMiss() throws Exception
    {
        String httpProxyHost = "";
        Integer httpProxyPort = 1234;

        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setHttpProxyHost(httpProxyHost);
        awsParamsDto.setHttpProxyPort(httpProxyPort);
       
        AmazonElasticMapReduceClient amazonElasticMapReduceClient = awsClientFactory.getEmrClient(awsParamsDto);
        
        AwsParamsDto awsParamsDto2 = new AwsParamsDto();
        awsParamsDto2.setHttpProxyHost("anotherone");
        awsParamsDto2.setHttpProxyPort(httpProxyPort);
        
        AmazonElasticMapReduceClient amazonElasticMapReduceClient2 = awsClientFactory.getEmrClient(awsParamsDto2);
        
        assertTrue(amazonElasticMapReduceClient != amazonElasticMapReduceClient2);
    }
    
    @Test
    public void getEmrClientCachingClear() throws Exception
    {
        String httpProxyHost = "";
        Integer httpProxyPort = 1234;

        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setHttpProxyHost(httpProxyHost);
        awsParamsDto.setHttpProxyPort(httpProxyPort);
              
        AmazonElasticMapReduceClient amazonElasticMapReduceClient = awsClientFactory.getEmrClient(awsParamsDto);
        
        // Clear the cache and retrieve the functions again.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();
        
        AwsParamsDto awsParamsDto2 = new AwsParamsDto();
        awsParamsDto2.setHttpProxyHost(httpProxyHost);
        awsParamsDto2.setHttpProxyPort(httpProxyPort);
        
        AmazonElasticMapReduceClient amazonElasticMapReduceClient2 = awsClientFactory.getEmrClient(awsParamsDto2);
        
        assertNotEquals(amazonElasticMapReduceClient, amazonElasticMapReduceClient2);
    }
    
    @Test
    public void getEC2ClientCachingHit() throws Exception
    {
        String httpProxyHost = "";
        Integer httpProxyPort = 1234;

        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setHttpProxyHost(httpProxyHost);
        awsParamsDto.setHttpProxyPort(httpProxyPort);
       
        AmazonEC2Client amazonEC2Client = awsClientFactory.getEc2Client(awsParamsDto);
        
        AwsParamsDto awsParamsDto2 = new AwsParamsDto();
        awsParamsDto2.setHttpProxyHost(httpProxyHost);
        awsParamsDto2.setHttpProxyPort(httpProxyPort);
        
        AmazonEC2Client amazonEC2Client2 = awsClientFactory.getEc2Client(awsParamsDto2);
        
        assertTrue(amazonEC2Client == amazonEC2Client2);
    }
    
    @Test
    public void getEC2ClientCachingHitMiss() throws Exception
    {
        String httpProxyHost = "";
        Integer httpProxyPort = 1234;

        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setHttpProxyHost(httpProxyHost);
        awsParamsDto.setHttpProxyPort(httpProxyPort);
       
        AmazonEC2Client amazonEC2Client = awsClientFactory.getEc2Client(awsParamsDto);
        
        AwsParamsDto awsParamsDto2 = new AwsParamsDto();
        awsParamsDto2.setHttpProxyHost("anotherone");
        awsParamsDto2.setHttpProxyPort(httpProxyPort);
        
        AmazonEC2Client amazonEC2Client2 = awsClientFactory.getEc2Client(awsParamsDto2);
        
        assertTrue(amazonEC2Client != amazonEC2Client2);
    }
    
    @Test
    public void getEC2ClientCachingClear() throws Exception
    {
        String httpProxyHost = "";
        Integer httpProxyPort = 1234;

        AwsParamsDto awsParamsDto = new AwsParamsDto();
        awsParamsDto.setHttpProxyHost(httpProxyHost);
        awsParamsDto.setHttpProxyPort(httpProxyPort);
              
        AmazonEC2Client ec2Client = awsClientFactory.getEc2Client(awsParamsDto);
       
        // Clear the cache and retrieve the functions again.
        cacheManager.getCache(DaoSpringModuleConfig.HERD_CACHE_NAME).clear();
        
        AwsParamsDto awsParamsDto2 = new AwsParamsDto();
        awsParamsDto2.setHttpProxyHost(httpProxyHost);
        awsParamsDto2.setHttpProxyPort(httpProxyPort);
        
        AmazonEC2Client ec2Client2 = awsClientFactory.getEc2Client(awsParamsDto2);
        
        assertNotEquals(ec2Client, ec2Client2);
    }
}
