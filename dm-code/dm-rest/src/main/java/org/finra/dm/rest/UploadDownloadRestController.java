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
package org.finra.dm.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.dm.model.dto.SecurityFunctions;
import org.finra.dm.model.jpa.NotificationEventTypeEntity;
import org.finra.dm.model.api.xml.BusinessObjectDataKey;
import org.finra.dm.model.api.xml.DownloadSingleInitiationResponse;
import org.finra.dm.model.api.xml.UploadSingleCredentialExtensionResponse;
import org.finra.dm.model.api.xml.UploadSingleInitiationRequest;
import org.finra.dm.model.api.xml.UploadSingleInitiationResponse;
import org.finra.dm.service.NotificationEventService;
import org.finra.dm.service.UploadDownloadService;
import org.finra.dm.service.helper.DmHelper;
import org.finra.dm.ui.constants.UiConstants;

/**
 * The REST controller that handles business object data REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
public class UploadDownloadRestController extends DmBaseController
{
    @Autowired
    private UploadDownloadService uploadDownloadService;

    @Autowired
    private DmHelper dmHelper;

    @Autowired
    private NotificationEventService notificationEventService;

    /**
     * Initiates a single file upload capability by creating the relative business object data instance in UPLOADING state and allowing write access to a
     * specific location in S3_MANAGED_LOADING_DOCK storage.
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
        BusinessObjectDataKey sourceBusinessObjectDataKey = dmHelper.getBusinessObjectDataKey(uploadSingleInitiationResponse.getSourceBusinessObjectData());
        BusinessObjectDataKey targetBusinessObjectDataKey = dmHelper.getBusinessObjectDataKey(uploadSingleInitiationResponse.getTargetBusinessObjectData());

        // Create business object data notification.
        notificationEventService
            .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN, sourceBusinessObjectDataKey);

        // Create business object data notification.
        notificationEventService
            .processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EVENT_TYPES_BDATA.BUS_OBJCT_DATA_RGSTN, targetBusinessObjectDataKey);

        return uploadSingleInitiationResponse;
    }

    /**
     * Initiates a download of a single file.
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
     * Extends the credentials for a previously initiated upload.
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
