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

import javax.servlet.ServletRequest;

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.StorageUnitNotificationFilter;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistration;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationCreateRequest;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationKeys;
import org.finra.herd.model.api.xml.StorageUnitNotificationRegistrationUpdateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.StorageUnitNotificationRegistrationService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles storage unit notification REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Storage Unit Notification Registration")
public class StorageUnitNotificationRegistrationRestController extends HerdBaseController
{
    public static final String STORAGE_UNIT_NOTIFICATIONS_URI_PREFIX = "/notificationRegistrations/storageUnitNotificationRegistrations";

    @Autowired
    private StorageUnitNotificationRegistrationService storageUnitNotificationRegistrationService;

    /**
     * Creates a new storage unit notification registration. <p>Requires WRITE permission on namespace</p> <p>Requires READ permission on filter namespace</p>
     * <p>Requires EXECUTE permission on ALL job action namespaces</p>
     *
     * @param request the information needed to create the storage unit notification registration
     *
     * @return the created storage unit notification registration
     */
    @RequestMapping(value = STORAGE_UNIT_NOTIFICATIONS_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_STORAGE_UNIT_NOTIFICATION_REGISTRATIONS_POST)
    public StorageUnitNotificationRegistration createStorageUnitNotificationRegistration(@RequestBody StorageUnitNotificationRegistrationCreateRequest request)
    {
        return storageUnitNotificationRegistrationService.createStorageUnitNotificationRegistration(request);
    }

    /**
     * Deletes an existing storage unit notification registration by key. <p>Requires WRITE permission on namespace</p>
     *
     * @param namespace the namespace of the storage unit notification registration
     * @param notificationName the name of the storage unit notification registration
     *
     * @return the storage unit notification registration that got deleted
     */
    @RequestMapping(value = STORAGE_UNIT_NOTIFICATIONS_URI_PREFIX + "/namespaces/{namespace}/notificationNames/{notificationName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_STORAGE_UNIT_NOTIFICATION_REGISTRATIONS_DELETE)
    public StorageUnitNotificationRegistration deleteStorageUnitNotification(@PathVariable("namespace") String namespace,
        @PathVariable("notificationName") String notificationName)
    {
        return storageUnitNotificationRegistrationService
            .deleteStorageUnitNotificationRegistration(new NotificationRegistrationKey(namespace, notificationName));
    }

    /**
     * Gets an existing storage unit notification registration by key. <p>Requires READ permission on namespace</p>
     *
     * @param namespace the namespace of the storage unit notification registration
     * @param notificationName the name of the storage unit notification registration
     *
     * @return the retrieved storage unit notification registration
     */
    @RequestMapping(value = STORAGE_UNIT_NOTIFICATIONS_URI_PREFIX + "/namespaces/{namespace}/notificationNames/{notificationName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGE_UNIT_NOTIFICATION_REGISTRATIONS_GET)
    public StorageUnitNotificationRegistration getStorageUnitNotificationRegistration(@PathVariable("namespace") String namespace,
        @PathVariable("notificationName") String notificationName)
    {
        return storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistration(new NotificationRegistrationKey(namespace, notificationName));
    }

    /**
     * Gets a list of keys for all existing storage unit notification registrations for the specified storage unit notification registration namespace.
     *
     * @param namespace the namespace of the storage unit notification registration
     *
     * @return the storage unit notification registration keys
     */
    @RequestMapping(value = STORAGE_UNIT_NOTIFICATIONS_URI_PREFIX + "/namespaces/{namespace}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGE_UNIT_NOTIFICATION_REGISTRATIONS_BY_NAMESPACE_GET)
    public StorageUnitNotificationRegistrationKeys getStorageUnitNotificationRegistrationsByNamespace(@PathVariable("namespace") String namespace)
    {
        return storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNamespace(namespace);
    }

    /**
     * <p> Gets a list of keys for all existing storage unit notification registrations that match the specified storage unit notification filter parameters.
     * </p> <p> This endpoint requires both namespace and name of the business object definition. </p> <p>Requires READ permission on business object definition
     * namespace</p>
     *
     * @param businessObjectDefinitionNamespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     * @param businessObjectFormatUsage the usage of the business object format
     * @param businessObjectFormatFileType the file type of the business object format
     * @param servletRequest the client request information
     *
     * @return the storage unit notification registration keys
     */
    @RequestMapping(value = STORAGE_UNIT_NOTIFICATIONS_URI_PREFIX, method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_STORAGE_UNIT_NOTIFICATION_REGISTRATIONS_BY_NOTIFICATION_FILTER_GET)
    public StorageUnitNotificationRegistrationKeys getStorageUnitNotificationRegistrationsByNotificationFilter(
        @RequestParam(value = "businessObjectDefinitionNamespace", required = true) String businessObjectDefinitionNamespace,
        @RequestParam(value = "businessObjectDefinitionName", required = true) String businessObjectDefinitionName,
        @RequestParam(value = "businessObjectFormatUsage", required = false) String businessObjectFormatUsage,
        @RequestParam(value = "businessObjectFormatFileType", required = false) String businessObjectFormatFileType, ServletRequest servletRequest)
    {
        // Ensure there are no duplicate query string parameters.
        validateNoDuplicateQueryStringParams(servletRequest.getParameterMap(), "businessObjectDefinitionNamespace", "businessObjectDefinitionName",
            "businessObjectFormatUsage", "businessObjectFormatFileType");

        // Invoke the service.
        return storageUnitNotificationRegistrationService.getStorageUnitNotificationRegistrationsByNotificationFilter(
            new StorageUnitNotificationFilter(businessObjectDefinitionNamespace, businessObjectDefinitionName, businessObjectFormatUsage,
                businessObjectFormatFileType, null, null, null, null));
    }

    /**
     * Updates an existing storage unit notification registration by key. <p>Requires WRITE permission on namespace</p> <p>Requires READ permission on filter
     * namespace</p> <p>Requires EXECUTE permission on ALL job action namespaces</p>
     *
     * @param namespace the namespace of the storage unit notification registration
     * @param notificationName the name of the storage unit notification registration
     * @param request the information needed to update the storage unit notification registration
     *
     * @return the updated storage unit notification registration
     */
    @RequestMapping(value = STORAGE_UNIT_NOTIFICATIONS_URI_PREFIX + "/namespaces/{namespace}/notificationNames/{notificationName}",
        method = RequestMethod.PUT, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_STORAGE_UNIT_NOTIFICATION_REGISTRATIONS_PUT)
    public StorageUnitNotificationRegistration updateStorageUnitNotificationRegistration(@PathVariable("namespace") String namespace,
        @PathVariable("notificationName") String notificationName, @RequestBody StorageUnitNotificationRegistrationUpdateRequest request)
    {
        return storageUnitNotificationRegistrationService
            .updateStorageUnitNotificationRegistration(new NotificationRegistrationKey(namespace, notificationName), request);
    }
}
