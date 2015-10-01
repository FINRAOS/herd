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
package org.finra.dm.service.systemjobs;

import java.util.List;

import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.model.jpa.StorageEntity;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.service.FileUploadCleanupService;
import org.finra.dm.service.helper.DmHelper;

/**
 * The file upload cleanup job.
 */
@Component(FileUploadCleanupJob.JOB_NAME)
@DisallowConcurrentExecution
public class FileUploadCleanupJob extends AbstractSystemJob
{
    public static final String JOB_NAME = "fileUploadCleanup";

    private static final Logger LOGGER = Logger.getLogger(FileUploadCleanupJob.class);

    @Autowired
    private FileUploadCleanupService fileUploadCleanupService;

    @Autowired
    protected DmHelper dmHelper;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException
    {
        // Log that the system job is started.
        LOGGER.info(String.format("Started \"%s\" system job.", JOB_NAME));

        // Get the parameter values.
        int thresholdMinutes = dmHelper.getParameterValueAsInteger(parameters, ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES);

        // Mark as DELETED any dangling business object data records with storage files in S3_MANAGED_LOADING_DOCK storage.
        try
        {
            fileUploadCleanupService.deleteBusinessObjectData(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, thresholdMinutes);
        }
        catch (Exception e)
        {
            // Log the exception.
            LOGGER.error("Failed to delete loading dock business object data.", e);
        }

        // Delete all orphaned multipart upload parts in all DM "managed" style S3 buckets.
        for (String storageName : StorageEntity.S3_MANAGED_STORAGES)
        {
            try
            {
                int abortedMultipartUploadsCount = fileUploadCleanupService.abortMultipartUploads(storageName, thresholdMinutes);
                LOGGER.info(String.format("Aborted %d expired multipart uploads in \"%s\" storage.", abortedMultipartUploadsCount, storageName));
            }
            catch (Exception e)
            {
                // Log the exception.
                LOGGER.error(String.format("Failed to abort expired multipart uploads in \"%s\" storage.", storageName), e);
            }
        }

        // Log that the system job is ended.
        LOGGER.info(String.format("Completed \"%s\" system job.", JOB_NAME));
    }

    @Override
    public void validateParameters(List<Parameter> parameters)
    {
        // This system job accepts only one optional parameter with an integer value.
        if (!CollectionUtils.isEmpty(parameters))
        {
            Assert.isTrue(parameters.size() == 1, String.format("Too many parameters are specified for \"%s\" system job.", JOB_NAME));
            Assert.isTrue(parameters.get(0).getName().equalsIgnoreCase(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES.getKey()),
                String.format("Parameter \"%s\" is not supported by \"%s\" system job.", parameters.get(0).getName(), FileUploadCleanupJob.JOB_NAME));
            dmHelper.getParameterValueAsInteger(parameters.get(0));
        }
    }

    @Override
    public JobDataMap getJobDataMap()
    {
        return getJobDataMap(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES);
    }

    @Override
    public String getCronExpression()
    {
        return configurationHelper.getProperty(ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_CRON_EXPRESSION);
    }
}
