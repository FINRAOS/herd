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
package org.finra.herd.dao.impl;

import java.io.File;
import java.io.FileNotFoundException;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.GlacierDao;
import org.finra.herd.dao.GlacierOperations;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.dto.GlacierArchiveTransferRequestParamsDto;
import org.finra.herd.model.dto.GlacierArchiveTransferResultsDto;

/**
 * The AWS Glacier DAO implementation.
 */
@Repository
public class GlacierDaoImpl implements GlacierDao
{
    private static final Logger LOGGER = Logger.getLogger(GlacierDaoImpl.class);

    @Autowired
    private AwsHelper awsHelper;

    @Autowired
    private GlacierOperations glacierOperations;

    /**
     * {@inheritDoc}
     */
    @Override
    public GlacierArchiveTransferResultsDto uploadArchive(GlacierArchiveTransferRequestParamsDto params) throws InterruptedException, FileNotFoundException
    {
        LOGGER.info(String.format("Uploading \"%s\" local file to AWS Glacier vault \"%s\" ...", params.getLocalFilePath(), params.getVaultName()));

        // Perform the transfer.
        GlacierArchiveTransferResultsDto results = performTransfer(params, new Transferer()
        {
            @Override
            public UploadResult performTransfer(ArchiveTransferManager archiveTransferManager) throws FileNotFoundException
            {
                return glacierOperations.upload(params.getVaultName(), null, new File(params.getLocalFilePath()), archiveTransferManager);
            }
        });

        LOGGER.info("Local file \"" + params.getLocalFilePath() + "\" contains " +
            results.getTotalBytesTransferred() + " byte(s) which was successfully transferred to \"" + params.getVaultName() +
            "\" Glacier vault in " + HerdDateUtils.formatDuration(results.getDurationMillis(), true));

        LOGGER.info(String.format("Overall transfer rate: %.2f kBytes/s (%.2f Mbits/s)",
            awsHelper.getTransferRateInKilobytesPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis()),
            awsHelper.getTransferRateInMegabitsPerSecond(results.getTotalBytesTransferred(), results.getDurationMillis())));

        return results;
    }

    /**
     * Gets a new Amazon Glacier client based on the specified parameters. The HTTP proxy information will be added if the host and port are specified in the
     * parameters.
     *
     * @param params the parameters
     *
     * @return the Amazon Glacier client
     */
    private AmazonGlacierClient getAmazonGlacierClient(GlacierArchiveTransferRequestParamsDto params)
    {
        // Construct a new AWS Glacier service client using the specified client configuration.
        // A credentials provider chain will be used that searches for credentials in this order:
        // - Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
        // - Java System Properties - aws.accessKeyId and aws.secretKey
        // - Instance Profile Credentials - delivered through the Amazon EC2 metadata service
        AmazonGlacierClient amazonGlacierClient = new AmazonGlacierClient(awsHelper.getClientConfiguration(params));

        // Set the optional endpoint if configured.
        if (StringUtils.isNotBlank(params.getGlacierEndpoint()))
        {
            LOGGER.info("Configured Glacier Endpoint: " + params.getGlacierEndpoint());
            amazonGlacierClient.setEndpoint(params.getGlacierEndpoint());
        }

        // Return the newly created client.
        return amazonGlacierClient;
    }

    /**
     * Performs a Glacier archive transfer.
     *
     * @param params the parameters
     * @param transferer a transferer that knows how to perform the transfer
     *
     * @return the Glacier archive transfer results
     * @throws InterruptedException if a problem is encountered
     * @throws FileNotFoundException if the specified file to upload doesn't exist
     */
    private GlacierArchiveTransferResultsDto performTransfer(final GlacierArchiveTransferRequestParamsDto params, Transferer transferer)
        throws InterruptedException, FileNotFoundException
    {
        // Create an archive transfer manager that will internally use an appropriate number of threads.
        ArchiveTransferManager archiveTransferManager = new ArchiveTransferManager(getAmazonGlacierClient(params), new DefaultAWSCredentialsProviderChain());

        // Get the archive file size.
        File localFile = new File(params.getLocalFilePath());
        long fileSizeBytes = localFile.length();

        // Start a stop watch to keep track of how long the transfer takes.
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        // Perform the transfer.
        UploadResult uploadResult = transferer.performTransfer(archiveTransferManager);

        // Stop the stop watch and create a results object.
        stopWatch.stop();

        // Create the results object and populate it with the standard data.
        GlacierArchiveTransferResultsDto results = new GlacierArchiveTransferResultsDto();
        results.setArchiveId(uploadResult.getArchiveId());
        results.setDurationMillis(stopWatch.getTime());
        results.setTotalBytesTransferred(fileSizeBytes);

        // Return the results.
        return results;
    }

    /**
     * An object that can perform a transfer using an archive transfer manager.
     */
    private interface Transferer
    {
        /**
         * Perform a transfer using the specified archive transfer manager.
         *
         * @param archiveTransferManager the archive transfer manager
         *
         * @return the result of the transfer
         */
        public UploadResult performTransfer(ArchiveTransferManager archiveTransferManager) throws FileNotFoundException;
    }
}
