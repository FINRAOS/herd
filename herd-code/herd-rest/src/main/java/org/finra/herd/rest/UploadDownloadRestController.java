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
package org.finra.herd.rest;

import java.util.Arrays;

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.DownloadSingleInitiationResponse;
import org.finra.herd.model.api.xml.StorageUnit;
import org.finra.herd.model.api.xml.UploadSingleCredentialExtensionResponse;
import org.finra.herd.model.api.xml.UploadSingleInitiationRequest;
import org.finra.herd.model.api.xml.UploadSingleInitiationResponse;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.UploadDownloadService;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles business object data REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Upload and Download")
public class UploadDownloadRestController extends HerdBaseController
{
    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private NotificationEventService notificationEventService;

    @Autowired
    private UploadDownloadService uploadDownloadService;

    /**
     * Initiates a single file upload capability by creating the relative business object data instance in UPLOADING state and allowing write access to a
     * specific location in S3_MANAGED_LOADING_DOCK storage. <p>Requires WRITE permission on namespace</p>
     *
     * @param uploadSingleInitiationRequest the information needed to initiate a file upload
     *
     * @return the upload single initiation response
     */
    @RequestMapping(value = "/upload/single/initiation", method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_UPLOAD_POST)
    public UploadSingleInitiationResponse initiateUploadSingle(@RequestBody UploadSingleInitiationRequest uploadSingleInitiationRequest)
    {
        UploadSingleInitiationResponse uploadSingleInitiationResponse = uploadDownloadService.initiateUploadSingle(uploadSingleInitiationRequest);

        // Trigger notifications.
        for (BusinessObjectData businessObjectData : Arrays
            .asList(uploadSingleInitiationResponse.getSourceBusinessObjectData(), uploadSingleInitiationResponse.getTargetBusinessObjectData()))
        {
            BusinessObjectDataKey businessObjectDataKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectData);

            // Create business object data notifications.
            for (NotificationEventTypeEntity.EventTypesBdata eventType : Arrays
                .asList(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_RGSTN, NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG))
            {
                notificationEventService
                    .processBusinessObjectDataNotificationEventAsync(eventType, businessObjectDataKey, businessObjectData.getStatus(), null);
            }

            // Create storage unit notifications.
            for (StorageUnit storageUnit : businessObjectData.getStorageUnits())
            {
                notificationEventService
                    .processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG, businessObjectDataKey,
                        storageUnit.getStorage().getName(), storageUnit.getStorageUnitStatus(), null);
            }
        }

        return uploadSingleInitiationResponse;
    }

    /**
     * Initiates a download of a single file. <p>Requires READ permission on namespace</p>
     *
     * @param namespace the namespace.
     * @param businessObjectDefinitionName the business object definition name.
     * @param businessObjectFormatUsage the business object format usage.
     * @param businessObjectFormatFileType the business object format file type.
     * @param businessObjectFormatVersion the business object format version.
     * @param partitionValue the partition value.
     * @param businessObjectDataVersion the business object data version.
     *
     * @return the download single initiation response.
     */
    @RequestMapping(value = "/download/single/initiation/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_DOWNLOAD_GET)
    public DownloadSingleInitiationResponse initiateDownloadSingle(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion)
    {
        return uploadDownloadService.initiateDownloadSingle(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
            businessObjectFormatVersion, partitionValue, businessObjectDataVersion);
    }

    /**
     * Extends the credentials for a previously initiated upload. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace.
     * @param businessObjectDefinitionName the business object definition name.
     * @param businessObjectFormatUsage the business object format usage.
     * @param businessObjectFormatFileType the business object format file type.
     * @param businessObjectFormatVersion the business object format version.
     * @param partitionValue the partition value.
     * @param businessObjectDataVersion the business object data version.
     *
     * @return the extended credentials.
     */
    @RequestMapping(value = "/upload/single/credential/extension/namespaces/{namespace}/businessObjectDefinitionNames/{businessObjectDefinitionName}" +
        "/businessObjectFormatUsages/{businessObjectFormatUsage}/businessObjectFormatFileTypes/{businessObjectFormatFileType}" +
        "/businessObjectFormatVersions/{businessObjectFormatVersion}/partitionValues/{partitionValue}" +
        "/businessObjectDataVersions/{businessObjectDataVersion}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_UPLOAD_EXTEND_CREDENTIALS_GET)
    public UploadSingleCredentialExtensionResponse extendUploadSingleCredentials(@PathVariable("namespace") String namespace,
        @PathVariable("businessObjectDefinitionName") String businessObjectDefinitionName,
        @PathVariable("businessObjectFormatUsage") String businessObjectFormatUsage,
        @PathVariable("businessObjectFormatFileType") String businessObjectFormatFileType,
        @PathVariable("businessObjectFormatVersion") Integer businessObjectFormatVersion, @PathVariable("partitionValue") String partitionValue,
        @PathVariable("businessObjectDataVersion") Integer businessObjectDataVersion)
    {
        return uploadDownloadService
            .extendUploadSingleCredentials(namespace, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion, partitionValue, businessObjectDataVersion);
    }
}
