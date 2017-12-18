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

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.service.ExpireRestoredBusinessObjectDataService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.ParameterHelper;

/**
 * The system job that expires restored business object data.
 */
@Component(ExpireRestoredBusinessObjectDataJob.JOB_NAME)
@DisallowConcurrentExecution
public class ExpireRestoredBusinessObjectDataJob extends AbstractSystemJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ExpireRestoredBusinessObjectDataJob.class);

    public static final String JOB_NAME = "expireRestoredBusinessObjectData";

    @Autowired
    private ExpireRestoredBusinessObjectDataService expireRestoredBusinessObjectDataService;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private ParameterHelper parameterHelper;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException
    {
        // Log that the system job is started.
        LOGGER.info("Started system job. systemJobName=\"{}\"", JOB_NAME);

        // Get the parameter values.
        int maxBusinessObjectDataInstancesToProcess =
            parameterHelper.getParameterValueAsInteger(parameters, ConfigurationValue.EXPIRE_RESTORED_BDATA_JOB_MAX_BDATA_INSTANCES);

        // Log the parameter values.
        LOGGER.info("systemJobName=\"{}\" {}={}", JOB_NAME, ConfigurationValue.EXPIRE_RESTORED_BDATA_JOB_MAX_BDATA_INSTANCES,
            maxBusinessObjectDataInstancesToProcess);

        // Continue the processing only if the maximum number of business object data instances
        // that is allowed to be processed in a single run of this system job is greater than zero.
        int processedBusinessObjectDataInstances = 0;
        if (maxBusinessObjectDataInstancesToProcess > 0)
        {
            // Select restored business object data that is already expired.
            List<BusinessObjectDataStorageUnitKey> storageUnitKeys =
                expireRestoredBusinessObjectDataService.getS3StorageUnitsToExpire(maxBusinessObjectDataInstancesToProcess);

            // Log the number of storage units selected for processing.
            LOGGER.info("Selected for processing S3 storage units. systemJobName=\"{}\" storageUnitCount={}", JOB_NAME, storageUnitKeys.size());

            // Try to expire each of the selected storage units.
            for (BusinessObjectDataStorageUnitKey storageUnitKey : storageUnitKeys)
            {
                try
                {
                    expireRestoredBusinessObjectDataService.expireS3StorageUnit(storageUnitKey);
                    processedBusinessObjectDataInstances += 1;
                }
                catch (RuntimeException runtimeException)
                {
                    // Log the exception.
                    LOGGER.error("Failed to expire a restored business object data. systemJobName=\"{}\" storageName=\"{}\" businessObjectDataKey={}", JOB_NAME,
                        storageUnitKey.getStorageName(),
                        jsonHelper.objectToJson(businessObjectDataHelper.createBusinessObjectDataKeyFromStorageUnitKey(storageUnitKey)), runtimeException);
                }
            }
        }

        // Log the number of finalized restores.
        LOGGER.info("Expired restored business object data instances. systemJobName=\"{}\" businessObjectDataCount={}", JOB_NAME,
            processedBusinessObjectDataInstances);

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
            Assert.isTrue(parameters.get(0).getName().equalsIgnoreCase(ConfigurationValue.EXPIRE_RESTORED_BDATA_JOB_MAX_BDATA_INSTANCES.getKey()),
                String.format("Parameter \"%s\" is not supported by \"%s\" system job.", parameters.get(0).getName(), JOB_NAME));
            parameterHelper.getParameterValueAsInteger(parameters.get(0));
        }
    }

    @Override
    public JobDataMap getJobDataMap()
    {
        return getJobDataMap(ConfigurationValue.EXPIRE_RESTORED_BDATA_JOB_MAX_BDATA_INSTANCES);
    }

    @Override
    public String getCronExpression()
    {
        return configurationHelper.getProperty(ConfigurationValue.EXPIRE_RESTORED_BDATA_JOB_CRON_EXPRESSION);
    }
}
