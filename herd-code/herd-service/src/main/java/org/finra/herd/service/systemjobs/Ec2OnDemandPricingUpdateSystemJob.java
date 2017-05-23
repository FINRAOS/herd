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

import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.Ec2OnDemandPricing;
import org.finra.herd.service.Ec2OnDemandPricingUpdateService;
import org.finra.herd.service.helper.ParameterHelper;

/**
 * The system job that updates EC2 on-demand pricing.
 */
@Component(Ec2OnDemandPricingUpdateSystemJob.JOB_NAME)
@DisallowConcurrentExecution
public class Ec2OnDemandPricingUpdateSystemJob extends AbstractSystemJob
{
    public static final String JOB_NAME = "ec2OnDemandPricingUpdate";

    private static final Logger LOGGER = LoggerFactory.getLogger(Ec2OnDemandPricingUpdateSystemJob.class);

    @Autowired
    private Ec2OnDemandPricingUpdateService ec2OnDemandPricingUpdateService;

    @Autowired
    private ParameterHelper parameterHelper;

    @Override
    public String getCronExpression()
    {
        return configurationHelper.getProperty(ConfigurationValue.EC2_ON_DEMAND_PRICING_UPDATE_JOB_CRON_EXPRESSION);
    }

    @Override
    public JobDataMap getJobDataMap()
    {
        return getJobDataMapWithoutParameters();
    }

    @Override
    public void validateParameters(List<Parameter> parameters)
    {
        // This system job accepts only one optional parameter.
        if (!org.springframework.util.CollectionUtils.isEmpty(parameters))
        {
            Assert.isTrue(parameters.size() == 1, String.format("Too many parameters are specified for \"%s\" system job.", JOB_NAME));
            Assert.isTrue(parameters.get(0).getName().equalsIgnoreCase(ConfigurationValue.EC2_ON_DEMAND_PRICING_UPDATE_JOB_EC2_PRICING_LIST_URL.getKey()),
                String.format("Parameter \"%s\" is not supported by \"%s\" system job.", parameters.get(0).getName(), JOB_NAME));
        }
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException
    {
        // Log that the system job is started.
        LOGGER.info("Started system job. systemJobName=\"{}\"", JOB_NAME);

        // Create an empty list of EC2 on-demand pricing entries.
        List<Ec2OnDemandPricing> ec2OnDemandPricingEntries = null;

        // Get the parameter values.
        String ec2PricingListUrl = parameterHelper.getParameterValue(parameters, ConfigurationValue.EC2_ON_DEMAND_PRICING_UPDATE_JOB_EC2_PRICING_LIST_URL);

        // Log the parameter values.
        LOGGER.info("systemJobName={} {}={}", JOB_NAME, ConfigurationValue.EC2_ON_DEMAND_PRICING_UPDATE_JOB_EC2_PRICING_LIST_URL, ec2PricingListUrl);

        // Retrieve the EC2 on-demand pricing information from AWS.
        try
        {
            // Retrieve the EC2 on-demand pricing information from AWS.
            ec2OnDemandPricingEntries = ec2OnDemandPricingUpdateService.getEc2OnDemandPricing(ec2PricingListUrl);

            // Log the number of storage units selected for processing.
            LOGGER.info("Retrieved {} EC2 on-demand pricing records from AWS. systemJobName=\"{}\"", ec2OnDemandPricingEntries.size(), JOB_NAME);
        }
        catch (Exception e)
        {
            // Log the exception.
            LOGGER.error("Failed to retrieve EC2 on-demand pricing information from AWS. systemJobName=\"{}\"", JOB_NAME, e);
        }

        // If pricing information was retrieved, update the EC2 on-demand pricing configured in the system.
        if (CollectionUtils.isNotEmpty(ec2OnDemandPricingEntries))
        {
            try
            {
                // Update the EC2 on-demand pricing configured in the system per retrieved pricing information.
                ec2OnDemandPricingUpdateService.updateEc2OnDemandPricing(ec2OnDemandPricingEntries);
            }
            catch (Exception e)
            {
                // Log the exception.
                LOGGER.error("Failed to update EC2 on-demand pricing. systemJobName=\"{}\"", JOB_NAME, e);
            }
        }

        // Log that the system job is ended.
        LOGGER.info("Completed system job. systemJobName=\"{}\"", JOB_NAME);
    }
}
