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

import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectDataStorageUnitKey;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.RelationalTableRegistrationService;

/**
 * The system job that updates schema for all relational tables registered in the system.
 */
@Component(RelationalTableSchemaUpdateSystemJob.JOB_NAME)
@DisallowConcurrentExecution
public class RelationalTableSchemaUpdateSystemJob extends AbstractSystemJob
{
    public static final String JOB_NAME = "relationalTableSchemaUpdate";

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationalTableSchemaUpdateSystemJob.class);

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private RelationalTableRegistrationService relationalTableRegistrationService;

    @Override
    public String getCronExpression()
    {
        return configurationHelper.getProperty(ConfigurationValue.RELATIONAL_TABLE_SCHEMA_UPDATE_JOB_CRON_EXPRESSION);
    }

    @Override
    public JobDataMap getJobDataMap()
    {
        return getJobDataMapWithoutParameters();
    }

    @Override
    public void validateParameters(List<Parameter> parameters)
    {
        // This system job accepts no parameters.
        Assert.isTrue(org.springframework.util.CollectionUtils.isEmpty(parameters), String.format("\"%s\" system job does not except parameters.", JOB_NAME));
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException
    {
        // Log that the system job is started.
        LOGGER.info("Started system job. systemJobName=\"{}\"", JOB_NAME);

        // Initialize a counter for number of schema updates.
        int updatedRelationalTableSchemaCount = 0;

        // Select relational table registration registered in the system.
        List<BusinessObjectDataStorageUnitKey> storageUnitKeys = relationalTableRegistrationService.getRelationalTableRegistrationsForSchemaUpdate();

        // Log the number of storage units selected for processing.
        LOGGER.info("Selected relational table registrations for schema update. systemJobName=\"{}\" storageUnitCount={}", JOB_NAME, storageUnitKeys.size());

        // Check if we need to update relational schema for each of the selected storage units.
        for (BusinessObjectDataStorageUnitKey storageUnitKey : storageUnitKeys)
        {
            try
            {
                if (relationalTableRegistrationService.processRelationalTableRegistrationForSchemaUpdate(storageUnitKey) != null)
                {
                    updatedRelationalTableSchemaCount += 1;
                }
            }
            catch (RuntimeException runtimeException)
            {
                // Log the exception.
                LOGGER.error("Failed to process relational table registration for schema update. systemJobName=\"{}\" storageUnitKey={}", JOB_NAME,
                    jsonHelper.objectToJson(storageUnitKey), runtimeException);
            }
        }

        // Log the number of finalized restores.
        LOGGER.info("Finished processing relational table registrations for schema update. systemJobName=\"{}\" updatedRelationalTableSchemaCount={}", JOB_NAME,
            updatedRelationalTableSchemaCount);


        // Log that the system job is ended.
        LOGGER.info("Completed system job. systemJobName=\"{}\"", JOB_NAME);
    }
}
