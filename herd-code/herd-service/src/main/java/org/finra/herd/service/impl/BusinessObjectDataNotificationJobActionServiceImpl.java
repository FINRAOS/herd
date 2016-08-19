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
package org.finra.herd.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.JobCreateRequest;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.BusinessObjectDataNotificationEventParamsDto;
import org.finra.herd.model.dto.NotificationEventParamsDto;
import org.finra.herd.model.jpa.BusinessObjectDataNotificationRegistrationEntity;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationJobActionEntity;
import org.finra.herd.model.jpa.NotificationTypeEntity;
import org.finra.herd.service.JobService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.NotificationRegistrationHelper;

/**
 * The abstract class for business object data notification job action service.
 */
public abstract class BusinessObjectDataNotificationJobActionServiceImpl extends NotificationActionServiceImpl
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessObjectDataNotificationJobActionServiceImpl.class);

    private static final String PARAM_BUSINESS_OBJECT_DATA_EVENT_TYPE = "notification_businessObjectDataEventType";

    private static final String PARAM_CORRELATION_DATA = "notification_correlationData";

    private static final String PARAM_BUSINESS_OBJECT_DATA = "notification_businessObjectData";

    private static final String PARAM_BUSINESS_OBJECT_DEFINITION_NAMESPACE = "notification_businessObjectDefinitionNamespace";

    private static final String PARAM_BUSINESS_OBJECT_DEFINITION_NAME = "notification_businessObjectDefinitionName";

    private static final String PARAM_BUSINESS_OBJECT_FORMAT_USAGE = "notification_businessObjectFormatUsage";

    private static final String PARAM_BUSINESS_OBJECT_FORMAT_FILE_TYPE = "notification_businessObjectFormatFileType";

    private static final String PARAM_BUSINESS_OBJECT_FORMAT_VERSION = "notification_businessObjectFormatVersion";

    private static final String PARAM_PARTITION_COLUMN_NAMES = "notification_partitionColumnNames";

    private static final String PARAM_PARTITION_VALUES = "notification_partitionValues";

    private static final String PARAM_BUSINESS_OBJECT_DATA_VERSION = "notification_businessObjectDataVersion";

    private static final String PARAM_NEW_BUSINESS_OBJECT_DATA_STATUS = "notification_newBusinessObjectDataStatus";

    private static final String PARAM_OLD_BUSINESS_OBJECT_DATA_STATUS = "notification_oldBusinessObjectDataStatus";

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private JobService jobService;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private NotificationRegistrationHelper notificationRegistrationHelper;

    protected BusinessObjectDataNotificationJobActionServiceImpl()
    {
        // Prevent classes from instantiating except sub-classes.
    }

    @Override
    public String getNotificationType()
    {
        return NotificationTypeEntity.NOTIFICATION_TYPE_BDATA;
    }

    @Override
    public String getNotificationActionType()
    {
        return NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN.name();
    }

    @Override
    public Object performNotificationAction(NotificationEventParamsDto notificationEventParams) throws Exception
    {
        if (notificationEventParams instanceof BusinessObjectDataNotificationEventParamsDto)
        {
            BusinessObjectDataNotificationEventParamsDto businessObjectDataNotificationEventParams =
                (BusinessObjectDataNotificationEventParamsDto) notificationEventParams;
            JobCreateRequest request = new JobCreateRequest();

            JobDefinitionEntity jobDefinitionEntity = businessObjectDataNotificationEventParams.getNotificationJobAction().getJobDefinition();

            request.setNamespace(jobDefinitionEntity.getNamespace().getCode());
            request.setJobName(jobDefinitionEntity.getName());
            request.setParameters(buildJobParameters(businessObjectDataNotificationEventParams));

            /*
             * Log enough information so we can trace back what notification registration triggered what workflow.
             * This also allows us to reproduce the workflow execution if needed by logging the entire jobCreateRequest in JSON format.
             */
            if (LOGGER.isInfoEnabled())
            {
                BusinessObjectDataNotificationRegistrationEntity businessObjectDataNotificationRegistration =
                    businessObjectDataNotificationEventParams.getBusinessObjectDataNotificationRegistration();
                LOGGER.info("Starting a job due to a notification. notificationRegistrationKey={} jobCreateRequest={}",
                    jsonHelper.objectToJson(notificationRegistrationHelper.getNotificationRegistrationKey(businessObjectDataNotificationRegistration)),
                    jsonHelper.objectToJson(request));
            }

            return jobService.createAndStartJob(request);
        }
        else
        {
            throw new IllegalStateException(
                "Notification event parameters DTO passed to the method must be an instance of BusinessObjectDataNotificationEventParamsDto.");
        }
    }

    @Override
    public String getIdentifyingInformation(NotificationEventParamsDto notificationEventParams, BusinessObjectDataHelper businessObjectDataHelper)
    {
        if (notificationEventParams instanceof BusinessObjectDataNotificationEventParamsDto)
        {
            BusinessObjectDataNotificationEventParamsDto businessObjectDataNotificationEventParams =
                (BusinessObjectDataNotificationEventParamsDto) notificationEventParams;

            return String.format("namespace: \"%s\", actionId: \"%s\" with " +
                businessObjectDataHelper.businessObjectDataKeyToString(
                    businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataNotificationEventParams.getBusinessObjectData())) +
                ", storageName: \"%s\"", businessObjectDataNotificationEventParams.getBusinessObjectDataNotificationRegistration().getNamespace().getCode(),
                businessObjectDataNotificationEventParams.getNotificationJobAction().getId(), businessObjectDataNotificationEventParams.getStorageName());
        }
        else
        {
            throw new IllegalStateException(
                "Notification event parameters DTO passed to the method must be an instance of BusinessObjectDataNotificationEventParamsDto.");
        }
    }

    private List<Parameter> buildJobParameters(BusinessObjectDataNotificationEventParamsDto businessObjectDataNotificationEventParams) throws IOException
    {
        List<Parameter> parameters = new ArrayList<>();

        BusinessObjectData businessObjectData = businessObjectDataNotificationEventParams.getBusinessObjectData();
        NotificationJobActionEntity notificationJobActionEntity = businessObjectDataNotificationEventParams.getNotificationJobAction();

        parameters.add(
            new Parameter(PARAM_NAMESPACE, businessObjectDataNotificationEventParams.getBusinessObjectDataNotificationRegistration().getNamespace().getCode()));
        parameters
            .add(new Parameter(PARAM_NOTIFICATION_NAME, businessObjectDataNotificationEventParams.getBusinessObjectDataNotificationRegistration().getName()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_DATA_EVENT_TYPE, businessObjectDataNotificationEventParams.getEventType()));
        parameters.add(new Parameter(PARAM_CORRELATION_DATA, notificationJobActionEntity.getCorrelationData()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_DATA, jsonHelper.objectToJson(businessObjectData)));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_DEFINITION_NAMESPACE, businessObjectData.getNamespace()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_DEFINITION_NAME, businessObjectData.getBusinessObjectDefinitionName()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_FORMAT_USAGE, businessObjectData.getBusinessObjectFormatUsage()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_FORMAT_FILE_TYPE, businessObjectData.getBusinessObjectFormatFileType()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_FORMAT_VERSION, Integer.toString(businessObjectData.getBusinessObjectFormatVersion())));
        parameters.add(new Parameter(PARAM_PARTITION_COLUMN_NAMES,
            herdStringHelper.buildStringWithDefaultDelimiter(businessObjectDataNotificationEventParams.getPartitionColumnNames())));
        parameters.add(new Parameter(PARAM_PARTITION_VALUES,
            herdStringHelper.buildStringWithDefaultDelimiter(businessObjectDataNotificationEventParams.getPartitionValues())));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_DATA_VERSION, Integer.toString(businessObjectData.getVersion())));
        parameters.add(new Parameter(PARAM_NEW_BUSINESS_OBJECT_DATA_STATUS, businessObjectDataNotificationEventParams.getNewBusinessObjectDataStatus()));
        parameters.add(new Parameter(PARAM_OLD_BUSINESS_OBJECT_DATA_STATUS, businessObjectDataNotificationEventParams.getOldBusinessObjectDataStatus()));

        return parameters;
    }
}
