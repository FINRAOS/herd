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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.AWSS3ControlClient;
import com.amazonaws.services.s3control.AWSS3ControlClientBuilder;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.HerdAWSCredentialsProvider;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;

@Component
public class AwsClientFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsClientFactory.class);

    @Autowired
    private AwsHelper awsHelper;

    /**
     * Creates a client for accessing Amazon SNS.
     *
     * @param awsParamsDto the AWS related parameters DTO that includes optional proxy information
     *
     * @return the Amazon SNS client
     */
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public AmazonSNS getAmazonSNSClient(AwsParamsDto awsParamsDto)
    {
        // Construct and return a new client to invoke service methods on Amazon SNS using default credentials provider chain.
        return AmazonSNSClientBuilder.standard().withClientConfiguration(awsHelper.getClientConfiguration(awsParamsDto))
            .withRegion(awsParamsDto.getAwsRegionName()).build();
    }

    /**
     * Creates a client for accessing Amazon SQS.
     *
     * @param awsParamsDto the AWS related parameters DTO that includes optional proxy information
     *
     * @return the Amazon SQS client
     */
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public AmazonSQS getAmazonSQSClient(AwsParamsDto awsParamsDto)
    {
        // Construct and return a new client to invoke service methods on Amazon SQS using default credentials provider chain.
        return AmazonSQSClientBuilder.standard().withClientConfiguration(awsHelper.getClientConfiguration(awsParamsDto))
            .withRegion(awsParamsDto.getAwsRegionName()).build();
    }

    /**
     * Creates a client for accessing Amazon EC2 service.
     *
     * @param awsParamsDto the AWS related parameters DTO that includes optional AWS credentials and proxy information
     *
     * @return the Amazon EC2 client
     */
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public AmazonEC2 getEc2Client(AwsParamsDto awsParamsDto)
    {
        // Get client configuration.
        ClientConfiguration clientConfiguration = awsHelper.getClientConfiguration(awsParamsDto);

        // If specified, use the AWS credentials passed in.
        if (StringUtils.isNotBlank(awsParamsDto.getAwsAccessKeyId()))
        {
            return AmazonEC2ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(
                    new BasicSessionCredentials(awsParamsDto.getAwsAccessKeyId(), awsParamsDto.getAwsSecretKey(), awsParamsDto.getSessionToken())))
                .withClientConfiguration(clientConfiguration).withRegion(awsParamsDto.getAwsRegionName()).build();
        }
        // Otherwise, use the default AWS credentials provider chain.
        else
        {
            return AmazonEC2ClientBuilder.standard().withClientConfiguration(clientConfiguration).withRegion(awsParamsDto.getAwsRegionName()).build();
        }
    }

    /**
     * Creates a client for accessing Amazon EMR service.
     *
     * @param awsParamsDto the AWS related parameters DTO that includes optional AWS credentials and proxy information
     *
     * @return the Amazon EMR client
     */
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public AmazonElasticMapReduce getEmrClient(AwsParamsDto awsParamsDto)
    {
        // Get client configuration.
        ClientConfiguration clientConfiguration = awsHelper.getClientConfiguration(awsParamsDto);

        // If specified, use the AWS credentials passed in.
        if (StringUtils.isNotBlank(awsParamsDto.getAwsAccessKeyId()))
        {
            return AmazonElasticMapReduceClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(
                    new BasicSessionCredentials(awsParamsDto.getAwsAccessKeyId(), awsParamsDto.getAwsSecretKey(), awsParamsDto.getSessionToken())))
                .withClientConfiguration(clientConfiguration).withRegion(awsParamsDto.getAwsRegionName()).build();
        }
        // Otherwise, use the default AWS credentials provider chain.
        else
        {
            return AmazonElasticMapReduceClientBuilder.standard().withClientConfiguration(clientConfiguration).withRegion(awsParamsDto.getAwsRegionName())
                .build();
        }
    }

    /**
     * Creates a cacheable client for AWS SES service with pluggable aws client params.
     *
     * @param awsParamsDto the specified aws parameters
     *
     * @return the Amazon SES client
     */
    @Cacheable(DaoSpringModuleConfig.HERD_CACHE_NAME)
    public AmazonSimpleEmailService getSesClient(AwsParamsDto awsParamsDto)
    {
        // Get client configuration
        ClientConfiguration clientConfiguration = awsHelper.getClientConfiguration(awsParamsDto);

        // If specified, use the AWS credentials passed in.
        if (StringUtils.isNotBlank(awsParamsDto.getAwsAccessKeyId()))
        {
            return AmazonSimpleEmailServiceClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(
                    new BasicSessionCredentials(awsParamsDto.getAwsAccessKeyId(), awsParamsDto.getAwsSecretKey(), awsParamsDto.getSessionToken())))
                .withClientConfiguration(clientConfiguration).withRegion(awsParamsDto.getAwsRegionName()).build();
        }
        // Otherwise, use the default AWS credentials provider chain.
        else
        {
            return AmazonSimpleEmailServiceClientBuilder.standard().withClientConfiguration(clientConfiguration).withRegion(awsParamsDto.getAwsRegionName())
                .build();
        }
    }

    /**
     * <p> Gets the {@link AWSCredentialsProvider} based on the credentials in the given parameters. </p> <p> Returns {@link
     * DefaultAWSCredentialsProviderChain} if either access or secret key is {@code null}. Otherwise returns a {@link StaticCredentialsProvider} with the
     * credentials. </p>
     *
     * @param params - Access parameters
     *
     * @return AWS credentials provider implementation
     */
    public AWSCredentialsProvider getAWSCredentialsProvider(S3FileTransferRequestParamsDto params)
    {
        List<AWSCredentialsProvider> providers = new ArrayList<>();
        String accessKey = params.getAwsAccessKeyId();
        String secretKey = params.getAwsSecretKey();
        if (accessKey != null && secretKey != null)
        {
            providers.add(new StaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        }
        for (HerdAWSCredentialsProvider herdAWSCredentialsProvider : params.getAdditionalAwsCredentialsProviders())
        {
            providers.add(new HerdAwsCredentialsProviderWrapper(herdAWSCredentialsProvider));
        }
        providers.add(new DefaultAWSCredentialsProviderChain());
        return new AWSCredentialsProviderChain(providers.toArray(new AWSCredentialsProvider[providers.size()]));
    }

    /**
     * Gets a new S3 client based on the specified parameters. The HTTP proxy information will be added if the host and port are specified in the parameters.
     *
     * @param params the parameters
     *
     * @return the Amazon S3 client.
     */
    public AmazonS3Client getAmazonS3Client(S3FileTransferRequestParamsDto params)
    {
        return getAmazonS3Client(params, null);
    }

    /**
     * Gets a new S3 client based on the specified parameters. The HTTP proxy information will be added if the host and port are specified in the parameters.
     *
     * @param params the parameters
     * @param awsCredentialsProvider the AWS credentials provider, may be null
     *
     * @return the Amazon S3 client.
     */
    public AmazonS3Client getAmazonS3Client(S3FileTransferRequestParamsDto params, AWSCredentialsProvider awsCredentialsProvider)
    {
        AmazonS3Client amazonS3Client;

        ClientConfiguration clientConfiguration = awsHelper.getS3ClientConfiguration(params);

        // Create an S3 client using passed in credentials provider or credentials.
        if (awsCredentialsProvider != null)
        {
            // Create an S3 client using specified AWS credentials provider.
            amazonS3Client = new AmazonS3Client(awsCredentialsProvider, clientConfiguration);
        }
        else if (StringUtils.isNotBlank(params.getAwsAccessKeyId()) && StringUtils.isNotBlank(params.getAwsSecretKey()) &&
            StringUtils.isNotBlank(params.getSessionToken()))
        {
            // Create an S3 client using basic session credentials.
            amazonS3Client = new AmazonS3Client(new BasicSessionCredentials(params.getAwsAccessKeyId(), params.getAwsSecretKey(), params.getSessionToken()),
                clientConfiguration);
        }
        else
        {
            // Create an S3 client using AWS credentials provider.
            amazonS3Client = new AmazonS3Client(getAWSCredentialsProvider(params), clientConfiguration);
        }

        // Set the optional endpoint, if specified.
        if (StringUtils.isNotBlank(params.getS3Endpoint()))
        {
            amazonS3Client.setEndpoint(params.getS3Endpoint());
        }
        // Otherwise, set AWS region, if specified.
        else if (StringUtils.isNotBlank(params.getAwsRegionName()))
        {
            amazonS3Client.setRegion(Region.getRegion(Regions.fromName(params.getAwsRegionName())));
        }

        // Return the newly created client.
        return amazonS3Client;
    }

    /**
     * Gets a new S3 control client based on the specified parameters.
     *
     * @param params the container object which contains s3 call specific parameters.
     *
     * @return the AWS S3 control client.
     */
    public AWSS3Control getAmazonS3Control(final S3FileTransferRequestParamsDto params)
    {
        ClientConfiguration clientConfiguration = awsHelper.getS3ClientConfiguration(params);

        AWSS3ControlClientBuilder s3ControlClientBuilder =
            AWSS3ControlClient.builder().withClientConfiguration(clientConfiguration).withRegion(params.getAwsRegionName());

        if (StringUtils.isNotBlank(params.getAwsAccessKeyId()) && StringUtils.isNotBlank(params.getAwsSecretKey()) &&
            StringUtils.isNotBlank(params.getSessionToken()))
        {
            LOGGER.info("Creating AWSS3Control with static credentials provider");
            // Create an S3 client using basic session credentials.
            s3ControlClientBuilder.withCredentials(
                new AWSStaticCredentialsProvider(new BasicSessionCredentials(params.getAwsAccessKeyId(), params.getAwsSecretKey(), params.getSessionToken())));
        }
        else
        {
            LOGGER.info("Creating AWSS3Control with HerdAwsCredentialsProvider");
            s3ControlClientBuilder.withCredentials(getAWSCredentialsProvider(params));
        }

        return s3ControlClientBuilder.build();
    }

    /**
     * Gets a transfer manager with the specified parameters including proxy host, proxy port, S3 access key, S3 secret key, and max threads.
     *
     * @param params the parameters.
     *
     * @return a newly created transfer manager.
     */
    public TransferManager getTransferManager(final S3FileTransferRequestParamsDto params)
    {
        // We are returning a new transfer manager each time it is called. Although the Javadocs of TransferManager say to share a single instance
        // if possible, this could potentially be a problem if TransferManager.shutdown(true) is called and underlying resources are not present when needed
        // for subsequent transfers.
        if (params.getMaxThreads() == null)
        {
            // Create a transfer manager that will internally use an appropriate number of threads.
            return new TransferManager(getAmazonS3Client(params));
        }
        else
        {
            // Create a transfer manager with our own executor configured with the specified total threads.
            LOGGER.info("Creating a transfer manager. fixedThreadPoolSize={}", params.getMaxThreads());
            return new TransferManager(getAmazonS3Client(params), Executors.newFixedThreadPool(params.getMaxThreads()));
        }
    }

    /**
     * A {@link AWSCredentialsProvider} which delegates to its wrapped {@link HerdAWSCredentialsProvider}
     */
    private static class HerdAwsCredentialsProviderWrapper implements AWSCredentialsProvider
    {
        private HerdAWSCredentialsProvider herdAWSCredentialsProvider;

        public HerdAwsCredentialsProviderWrapper(HerdAWSCredentialsProvider herdAWSCredentialsProvider)
        {
            this.herdAWSCredentialsProvider = herdAWSCredentialsProvider;
        }

        @Override
        public AWSCredentials getCredentials()
        {
            AwsCredential herdAwsCredential = herdAWSCredentialsProvider.getAwsCredential();
            return new BasicSessionCredentials(herdAwsCredential.getAwsAccessKey(), herdAwsCredential.getAwsSecretKey(),
                herdAwsCredential.getAwsSessionToken());
        }

        @Override
        public void refresh()
        {
            // No need to implement this. AWS doesn't use this.
        }
    }
}
