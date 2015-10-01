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
package org.finra.dm.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.dm.dao.config.DaoSpringModuleConfig;
import org.finra.dm.model.dto.BusinessObjectDataNotificationEventParamsDto;
import org.finra.dm.model.dto.NotificationEventParamsDto;
import org.finra.dm.model.jpa.JobDefinitionEntity;
import org.finra.dm.model.jpa.NotificationEventTypeEntity;
import org.finra.dm.model.jpa.NotificationJobActionEntity;
import org.finra.dm.model.jpa.NotificationTypeEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.JobCreateRequest;
import org.finra.dm.model.api.xml.Parameter;
import org.finra.dm.service.JobService;
import org.finra.dm.service.helper.DmHelper;

/**
 * The business object data notification job action service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.DM_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDataNotificationJobActionServiceImpl extends NotificationActionServiceImpl
{
    /**
     * The parameters that are sent along with the notification.
     */
    private static final String PARAM_BUSINESS_OBJECT_DATA_EVENT_TYPE = "notification_businessObjectDataEventType";
    private static final String PARAM_CORRELATION_DATA = "notification_correlationData";
    private static final String PARAM_BUSINESS_OBJECT_DEFINITION_NAMESPACE = "notification_businessObjectDefinitionNamespace";
    private static final String PARAM_BUSINESS_OBJECT_DEFINITION_NAME = "notification_businessObjectDefinitionName";
    private static final String PARAM_BUSINESS_OBJECT_FORMAT_USAGE = "notification_businessObjectFormatUsage";
    private static final String PARAM_BUSINESS_OBJECT_FORMAT_FILE_TYPE = "notification_businessObjectFormatFileType";
    private static final String PARAM_BUSINESS_OBJECT_FORMAT_VERSION = "notification_businessObjectFormatVersion";
    private static final String PARAM_PARTITION_VALUE = "notification_partitionValue";
    private static final String PARAM_SUB_PARTITION_VALUES = "notification_subPartitionValues";
    private static final String PARAM_BUSINESS_OBJECT_DATA_VERSION = "notification_businessObjectDataVersion";

    @Autowired
    private JobService jobService;

    @Override
    public String getNotificationType()
    {
        return NotificationTypeEntity.NOTIFICATION_TYPE_BDATA;
    }

    @Override
    public String getNotificationActionType()
    {
        return NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN.name();
    }

    @Override
    public Object performNotificationAction(NotificationEventParamsDto notificationEventParams) throws Exception
    {
        BusinessObjectDataNotificationEventParamsDto businessObjectDataNotificationEventParams =
            (BusinessObjectDataNotificationEventParamsDto) notificationEventParams;
        JobCreateRequest request = new JobCreateRequest();

        JobDefinitionEntity jobDefinitionEntity = businessObjectDataNotificationEventParams.getNotificationJobAction().getJobDefinition();

        request.setNamespace(jobDefinitionEntity.getNamespace().getCode());
        request.setJobName(jobDefinitionEntity.getName());
        request.setParameters(buildJobParameters(businessObjectDataNotificationEventParams));

        return jobService.createAndStartJob(request, true);
    }

    @Override
    public String getIdentifyingInformation(NotificationEventParamsDto notificationEventParams, DmHelper dmHelper)
    {
        BusinessObjectDataNotificationEventParamsDto businessObjectDataNotificationEventParams =
            (BusinessObjectDataNotificationEventParamsDto) notificationEventParams;

        return String.format("namespace: \"%s\", actionId: \"%s\" with " +
            dmHelper.businessObjectDataKeyToString(businessObjectDataNotificationEventParams.getBusinessObjectDataKey()) +
            ", storageName: \"%s\"", businessObjectDataNotificationEventParams.getBusinessObjectDataNotificationRegistration().getNamespace().getCode(),
            businessObjectDataNotificationEventParams.getNotificationJobAction().getId(), businessObjectDataNotificationEventParams.getStorageName());
    }

    private List<Parameter> buildJobParameters(BusinessObjectDataNotificationEventParamsDto businessObjectDataNotificationEventParams)
    {
        List<Parameter> parameters = new ArrayList<>();

        BusinessObjectDataKey businessObjectDataKey = businessObjectDataNotificationEventParams.getBusinessObjectDataKey();
        NotificationJobActionEntity notificationJobActionEntity = businessObjectDataNotificationEventParams.getNotificationJobAction();

        parameters.add(
            new Parameter(PARAM_NAMESPACE, businessObjectDataNotificationEventParams.getBusinessObjectDataNotificationRegistration().getNamespace().getCode()));
        parameters
            .add(new Parameter(PARAM_NOTIFICATION_NAME, businessObjectDataNotificationEventParams.getBusinessObjectDataNotificationRegistration().getName()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_DATA_EVENT_TYPE, businessObjectDataNotificationEventParams.getEventType()));
        parameters.add(new Parameter(PARAM_CORRELATION_DATA, notificationJobActionEntity.getCorrelationData()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_DEFINITION_NAMESPACE, businessObjectDataKey.getNamespace()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_DEFINITION_NAME, businessObjectDataKey.getBusinessObjectDefinitionName()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_FORMAT_USAGE, businessObjectDataKey.getBusinessObjectFormatUsage()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_FORMAT_FILE_TYPE, businessObjectDataKey.getBusinessObjectFormatFileType()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_FORMAT_VERSION, businessObjectDataKey.getBusinessObjectFormatVersion().toString()));
        parameters.add(new Parameter(PARAM_PARTITION_VALUE, businessObjectDataKey.getPartitionValue()));
        parameters.add(new Parameter(PARAM_SUB_PARTITION_VALUES, dmHelper.buildStringWithDefaultDelimiter(businessObjectDataKey.getSubPartitionValues())));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_DATA_VERSION, businessObjectDataKey.getBusinessObjectDataVersion().toString()));

        return parameters;
    }
}