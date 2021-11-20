package org.finra.herd.dao.helper;

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
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3control.AWSS3Control;
import com.amazonaws.services.s3control.AWSS3ControlClient;
import com.amazonaws.services.s3control.AWSS3ControlClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.RetryPolicyFactory;
import org.finra.herd.model.api.xml.AwsCredential;
import org.finra.herd.model.dto.HerdAWSCredentialsProvider;
import org.finra.herd.model.dto.S3FileTransferRequestParamsDto;

@Component
public class AWSClientFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AWSClientFactory.class);
    final String SIGNER_OVERRIDE_V4 = "AWSS3V4SignerType";
    @Autowired
    private RetryPolicyFactory retryPolicyFactory;

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

    private ClientConfiguration getClientConfiguration(S3FileTransferRequestParamsDto params)
    {
        ClientConfiguration clientConfiguration = new ClientConfiguration().withRetryPolicy(retryPolicyFactory.getRetryPolicy());

        // Set the proxy configuration, if proxy is specified.
        if (StringUtils.isNotBlank(params.getHttpProxyHost()) && params.getHttpProxyPort() != null)
        {
            clientConfiguration.setProxyHost(params.getHttpProxyHost());
            clientConfiguration.setProxyPort(params.getHttpProxyPort());
        }

        // Sign all S3 API's with V4 signing.
        // AmazonS3Client.upgradeToSigV4 already has some scenarios where it will "upgrade" the signing approach to use V4 if not already present (e.g.
        // GetObjectRequest and KMS PutObjectRequest), but setting it here (especially when KMS is used) will ensure it isn't missed when required (e.g.
        // copying objects between KMS encrypted buckets). Otherwise, AWS will return a bad request error and retry which isn't desirable.
        clientConfiguration.setSignerOverride(SIGNER_OVERRIDE_V4);

        // Set the optional socket timeout, if configured.
        if (params.getSocketTimeout() != null)
        {
            clientConfiguration.setSocketTimeout(params.getSocketTimeout());
        }

        return clientConfiguration;
    }

    /**
     * Gets a new S3 client based on the specified parameters. The HTTP proxy information will be added if the host and port are specified in the parameters.
     *
     * @param params the parameters.
     *
     * @return the Amazon S3 client.
     */
    public AmazonS3Client getAmazonS3(S3FileTransferRequestParamsDto params)
    {
        AmazonS3Client amazonS3Client;

        ClientConfiguration clientConfiguration = getClientConfiguration(params);

        // Create an S3 client using passed in credentials and HTTP proxy information.
        if (StringUtils.isNotBlank(params.getAwsAccessKeyId()) && StringUtils.isNotBlank(params.getAwsSecretKey()) &&
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

    public AWSS3Control getAmazonS3Control(final S3FileTransferRequestParamsDto params)
    {
        ClientConfiguration clientConfiguration = getClientConfiguration(params);

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

        // Set the optional endpoint, if specified.
        if (StringUtils.isNotBlank(params.getS3Endpoint()))
        {
            s3ControlClientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(params.getS3Endpoint(), params.getAwsRegionName()));
        }
        // Otherwise, set AWS region, if specified.
        else if (StringUtils.isNotBlank(params.getAwsRegionName()))
        {
            s3ControlClientBuilder.withRegion(Regions.fromName(params.getAwsRegionName()));
        }

        if (StringUtils.isNotBlank(params.getS3Endpoint()))
        {
            s3ControlClientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(params.getS3Endpoint(), params.getAwsRegionName()));
        }

        s3ControlClientBuilder.withClientConfiguration(clientConfiguration);

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
            return new TransferManager(getAmazonS3(params));
        }
        else
        {
            // Create a transfer manager with our own executor configured with the specified total threads.
            LOGGER.info("Creating a transfer manager. fixedThreadPoolSize={}", params.getMaxThreads());
            return new TransferManager(getAmazonS3(params), Executors.newFixedThreadPool(params.getMaxThreads()));
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
