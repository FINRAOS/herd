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
package org.finra.herd.service.systemjobs;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.service.FileUploadCleanupService;
import org.finra.herd.service.helper.ParameterHelper;

/**
 * The file upload cleanup job.
 */
@Component(FileUploadCleanupJob.JOB_NAME)
@DisallowConcurrentExecution
public class FileUploadCleanupJob extends AbstractSystemJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadCleanupJob.class);

    public static final String JOB_NAME = "fileUploadCleanup";

    @Autowired
    private FileUploadCleanupService fileUploadCleanupService;

    @Autowired
    private ParameterHelper parameterHelper;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException
    {
        // Log that the system job is started.
        LOGGER.info("Started system job. systemJobName=\"{}\"", JOB_NAME);

        // Get the parameter values.
        int thresholdMinutes = parameterHelper.getParameterValueAsInteger(parameters, ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES);

        // Log the parameter values.
        LOGGER.info("systemJobName={} {}={}", JOB_NAME, ConfigurationValue.FILE_UPLOAD_CLEANUP_JOB_THRESHOLD_MINUTES, thresholdMinutes);

        // Mark as DELETED any dangling business object data records with storage files in S3_MANAGED_LOADING_DOCK storage.
        try
        {
            List<BusinessObjectDataKey> businessObjectDataKeys =
                fileUploadCleanupService.deleteBusinessObjectData(StorageEntity.MANAGED_LOADING_DOCK_STORAGE, thresholdMinutes);
            LOGGER.info("Deleted {} instances of loading dock business object data. systemJobName=\"{}\" storageName=\"{}\"",
                CollectionUtils.size(businessObjectDataKeys), JOB_NAME, StorageEntity.MANAGED_LOADING_DOCK_STORAGE);
        }
        catch (Exception e)
        {
            // Log the exception.
            LOGGER.error("Failed to delete loading dock business object data. systemJobName=\"{}\"", JOB_NAME, e);
        }

        // Log that the system job is ended.
        LOGGER.info("Completed system job. systemJobName=\"{}\"", JOB_NAME);
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
            parameterHelper.getParameterValueAsInteger(parameters.get(0));
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
