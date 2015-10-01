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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.quartz.JobDataMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.util.CollectionUtils;

import org.finra.dm.core.helper.ConfigurationHelper;
import org.finra.dm.model.dto.ConfigurationValue;
import org.finra.dm.model.api.xml.Parameter;

/**
 * This is an abstract class that represents a scheduled system job.
 */
public abstract class AbstractSystemJob extends QuartzJobBean
{
    public static final String SYSTEM_JOB_PARAMETERS = "parameters";

    public static final String CRON_TRIGGER_SUFFIX = "CronTrigger";

    public static final String RUN_ONCE_TRIGGER_SUFFIX = "RunOnceTrigger";

    protected Map<String, String> parameters;

    @Autowired
    protected ConfigurationHelper configurationHelper;

    /**
     * Setter called after a system job is instantiated with the value from the JobDetailFactoryBean.
     */
    public void setParameters(Map<String, String> parameters)
    {
        this.parameters = parameters;
    }

    /**
     * Validates parameters passed to the system job. This method assumes that parameter names are stored in lower case.
     *
     * @param parameters the list of parameters to be validated
     */
    public abstract void validateParameters(List<Parameter> parameters);

    /**
     * Gets a cron expression for the system job.
     *
     * @return the cron expression for the system job
     */
    public abstract String getCronExpression();

    /**
     * Gets a job data map.
     *
     * @return the job data map
     */
    public abstract JobDataMap getJobDataMap();

    /**
     * Returns a job data map that contains a single hash map that is loaded with name-value pairs as per specified list of parameters. The parameter names are
     * loaded/stored in lowercase.
     *
     * @param parameters the list of parameters
     *
     * @return the job data map that contains a single hash map loaded with job parameters
     */
    public JobDataMap getJobDataMap(List<Parameter> parameters)
    {
        // Create a hash map and load it with the specified parameters.
        Map<String, String> jobParameters = new HashMap<>();
        if (!CollectionUtils.isEmpty(parameters))
        {
            for (Parameter parameter : parameters)
            {
                jobParameters.put(parameter.getName().toLowerCase(), parameter.getValue());
            }
        }

        // Create a job data map and populate it with the parameters.
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(SYSTEM_JOB_PARAMETERS, jobParameters);

        return jobDataMap;
    }

    /**
     * Returns a job data map that contains a single hash map without any parameters.
     *
     * @return the job data map that contains a single hash map loaded with job parameters
     */
    protected JobDataMap getJobDataMapWithoutParameters()
    {
        return getJobDataMap(new ConfigurationValue[0]);
    }

    /**
     * Returns a job data map that contains a single hash map that is loaded with name-value pairs as per specified list of configuration values. The parameter
     * names are loaded/stored in lowercase.
     *
     * @param configurationValues the list of configuration values
     *
     * @return the job data map that contains a single hash map loaded with job parameters
     */
    protected JobDataMap getJobDataMap(ConfigurationValue... configurationValues)
    {
        // Create a hash map and load it with the specified configuration values.
        Map<String, String> jobParameters = new HashMap<>();
        for (ConfigurationValue configurationValue : configurationValues)
        {
            String parameterValue = configurationHelper.getProperty(configurationValue);
            jobParameters.put(configurationValue.getKey().toLowerCase(), parameterValue);
        }

        // Create a job data map and populate it with the parameters.
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(SYSTEM_JOB_PARAMETERS, jobParameters);

        return jobDataMap;
    }
}
