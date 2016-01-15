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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.StoragePolicySelection;
import org.finra.herd.service.StoragePolicySelectorService;
import org.finra.herd.service.helper.HerdHelper;

/**
 * The storage policy selector job.
 */
@Component(StoragePolicySelectorJob.JOB_NAME)
@DisallowConcurrentExecution
public class StoragePolicySelectorJob extends AbstractSystemJob
{
    public static final String JOB_NAME = "storagePolicySelector";

    private static final Logger LOGGER = Logger.getLogger(StoragePolicySelectorJob.class);

    @Autowired
    protected HerdHelper herdHelper;

    @Autowired
    private StoragePolicySelectorService storagePolicySelectorService;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException
    {
        // Log that the system job is started.
        LOGGER.info(String.format("Started \"%s\" system job.", JOB_NAME));

        // Get the SQS queue name from the system configuration.
        String sqsQueueName = configurationHelper.getProperty(ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE_NAME);

        // Throw IllegalStateException if SQS queue name is undefined.
        if (StringUtils.isBlank(sqsQueueName))
        {
            throw new IllegalStateException(String
                .format("SQS queue name not found for \"%s\" system job. Ensure the \"%s\" configuration entry is configured.", JOB_NAME,
                    ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_SQS_QUEUE_NAME.getKey()));
        }

        // Get the parameter values.
        int maxBusinessObjectDataInstancesToSelect =
            herdHelper.getParameterValueAsInteger(parameters, ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_MAX_BDATA_INSTANCES);

        // Log the parameter values.
        LOGGER.info(String.format("\"%s\" system job: %s=%d", JOB_NAME, ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_MAX_BDATA_INSTANCES,
            maxBusinessObjectDataInstancesToSelect));

        // Continue the storage policies processing only if the maximum number of business object data instances
        // that is allowed to be selected in a single run of this system job is greater than zero.
        List<StoragePolicySelection> storagePolicySelections = new ArrayList<>();
        if (maxBusinessObjectDataInstancesToSelect > 0)
        {
            storagePolicySelections = storagePolicySelectorService.execute(sqsQueueName, maxBusinessObjectDataInstancesToSelect);
        }

        // Log the number of selections.
        LOGGER.info(String.format("Selected %d business object data instances per storage policies configured in the system.", storagePolicySelections.size()));

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
            Assert.isTrue(parameters.get(0).getName().equalsIgnoreCase(ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_MAX_BDATA_INSTANCES.getKey()),
                String.format("Parameter \"%s\" is not supported by \"%s\" system job.", parameters.get(0).getName(), StoragePolicySelectorJob.JOB_NAME));
            herdHelper.getParameterValueAsInteger(parameters.get(0));
        }
    }

    @Override
    public JobDataMap getJobDataMap()
    {
        return getJobDataMap(ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_MAX_BDATA_INSTANCES);
    }

    @Override
    public String getCronExpression()
    {
        return configurationHelper.getProperty(ConfigurationValue.STORAGE_POLICY_SELECTOR_JOB_CRON_EXPRESSION);
    }
}
