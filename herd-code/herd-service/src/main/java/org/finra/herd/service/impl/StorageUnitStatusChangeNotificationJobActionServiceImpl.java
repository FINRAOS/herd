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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.JobCreateRequest;
import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.model.dto.NotificationEventParamsDto;
import org.finra.herd.model.dto.StorageUnitNotificationEventParamsDto;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.NotificationJobActionEntity;
import org.finra.herd.model.jpa.NotificationTypeEntity;
import org.finra.herd.model.jpa.StorageUnitNotificationRegistrationEntity;
import org.finra.herd.service.JobService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.NotificationRegistrationHelper;

/**
 * The storage unit status change notification job action service.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class StorageUnitStatusChangeNotificationJobActionServiceImpl extends NotificationActionServiceImpl
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageUnitStatusChangeNotificationJobActionServiceImpl.class);

    private static final String PARAM_NEW_STORAGE_UNIT_STATUS = "notification_newStorageUnitStatus";

    private static final String PARAM_OLD_STORAGE_UNIT_STATUS = "notification_oldStorageUnitStatus";

    private static final String PARAM_STORAGE_NAME = "notification_storageName";

    private static final String PARAM_STORAGE_UNIT_EVENT_TYPE = "notification_storageUnitEventType";

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private JobService jobService;

    @Autowired
    private JsonHelper jsonHelper;

    @Autowired
    private NotificationRegistrationHelper notificationRegistrationHelper;

    @Override
    public String getIdentifyingInformation(NotificationEventParamsDto notificationEventParams, BusinessObjectDataHelper businessObjectDataHelper)
    {
        if (notificationEventParams instanceof StorageUnitNotificationEventParamsDto)
        {
            StorageUnitNotificationEventParamsDto storageUnitNotificationEventParams = (StorageUnitNotificationEventParamsDto) notificationEventParams;

            return String.format("namespace: \"%s\", actionId: \"%s\" with " +
                businessObjectDataHelper.businessObjectDataKeyToString(
                    businessObjectDataHelper.getBusinessObjectDataKey(storageUnitNotificationEventParams.getBusinessObjectData())) +
                ", storageName: \"%s\"", storageUnitNotificationEventParams.getStorageUnitNotificationRegistration().getNamespace().getCode(),
                storageUnitNotificationEventParams.getNotificationJobAction().getId(), storageUnitNotificationEventParams.getStorageName());
        }
        else
        {
            throw new IllegalStateException(
                "Notification event parameters DTO passed to the method must be an instance of StorageUnitNotificationEventParamsDto.");
        }
    }

    @Override
    public String getNotificationActionType()
    {
        return NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG.name();
    }

    @Override
    public String getNotificationType()
    {
        return NotificationTypeEntity.NOTIFICATION_TYPE_STORAGE_UNIT;
    }

    @Override
    public Object performNotificationAction(NotificationEventParamsDto notificationEventParams) throws Exception
    {
        if (notificationEventParams instanceof StorageUnitNotificationEventParamsDto)
        {
            StorageUnitNotificationEventParamsDto storageUnitNotificationEventParams = (StorageUnitNotificationEventParamsDto) notificationEventParams;
            JobCreateRequest request = new JobCreateRequest();

            JobDefinitionEntity jobDefinitionEntity = storageUnitNotificationEventParams.getNotificationJobAction().getJobDefinition();

            request.setNamespace(jobDefinitionEntity.getNamespace().getCode());
            request.setJobName(jobDefinitionEntity.getName());
            request.setParameters(buildJobParameters(storageUnitNotificationEventParams));

            /*
             * Log enough information so we can trace back what notification registration triggered what workflow.
             * This also allows us to reproduce the workflow execution if needed by logging the entire jobCreateRequest in JSON format.
             */
            if (LOGGER.isInfoEnabled())
            {
                StorageUnitNotificationRegistrationEntity storageUnitNotificationRegistration =
                    storageUnitNotificationEventParams.getStorageUnitNotificationRegistration();
                LOGGER.info("Starting a job due to a notification. notificationRegistrationKey={} jobCreateRequest={}",
                    jsonHelper.objectToJson(notificationRegistrationHelper.getNotificationRegistrationKey(storageUnitNotificationRegistration)),
                    jsonHelper.objectToJson(request));
            }

            return jobService.createAndStartJob(request);
        }
        else
        {
            throw new IllegalStateException(
                "Notification event parameters DTO passed to the method must be an instance of StorageUnitNotificationEventParamsDto.");
        }
    }

    private List<Parameter> buildJobParameters(StorageUnitNotificationEventParamsDto storageUnitNotificationEventParams) throws IOException
    {
        List<Parameter> parameters = new ArrayList<>();

        BusinessObjectData businessObjectData = storageUnitNotificationEventParams.getBusinessObjectData();
        NotificationJobActionEntity notificationJobActionEntity = storageUnitNotificationEventParams.getNotificationJobAction();

        parameters.add(new Parameter(PARAM_NAMESPACE, storageUnitNotificationEventParams.getStorageUnitNotificationRegistration().getNamespace().getCode()));
        parameters.add(new Parameter(PARAM_NOTIFICATION_NAME, storageUnitNotificationEventParams.getStorageUnitNotificationRegistration().getName()));
        parameters.add(new Parameter(PARAM_STORAGE_UNIT_EVENT_TYPE, storageUnitNotificationEventParams.getEventType()));
        parameters.add(new Parameter(PARAM_CORRELATION_DATA, notificationJobActionEntity.getCorrelationData()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_DATA, jsonHelper.objectToJson(businessObjectData)));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_DEFINITION_NAMESPACE, businessObjectData.getNamespace()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_DEFINITION_NAME, businessObjectData.getBusinessObjectDefinitionName()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_FORMAT_USAGE, businessObjectData.getBusinessObjectFormatUsage()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_FORMAT_FILE_TYPE, businessObjectData.getBusinessObjectFormatFileType()));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_FORMAT_VERSION, Integer.toString(businessObjectData.getBusinessObjectFormatVersion())));
        parameters.add(new Parameter(PARAM_PARTITION_COLUMN_NAMES,
            herdStringHelper.buildStringWithDefaultDelimiter(storageUnitNotificationEventParams.getPartitionColumnNames())));
        parameters.add(
            new Parameter(PARAM_PARTITION_VALUES, herdStringHelper.buildStringWithDefaultDelimiter(storageUnitNotificationEventParams.getPartitionValues())));
        parameters.add(new Parameter(PARAM_BUSINESS_OBJECT_DATA_VERSION, Integer.toString(businessObjectData.getVersion())));
        parameters.add(new Parameter(PARAM_STORAGE_NAME, storageUnitNotificationEventParams.getStorageName()));
        parameters.add(new Parameter(PARAM_NEW_STORAGE_UNIT_STATUS, storageUnitNotificationEventParams.getNewStorageUnitStatus()));
        parameters.add(new Parameter(PARAM_OLD_STORAGE_UNIT_STATUS, storageUnitNotificationEventParams.getOldStorageUnitStatus()));

        return parameters;
    }
}
