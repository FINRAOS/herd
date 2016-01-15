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

import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistration;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationCreateRequest;
import org.finra.herd.model.api.xml.NotificationRegistrationKey;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationKeys;
import org.finra.herd.model.api.xml.BusinessObjectDataNotificationRegistrationUpdateRequest;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.BusinessObjectDataNotificationRegistrationService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller that handles business object data notification REST requests.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Business Object Data Notification Registration")
public class BusinessObjectDataNotificationRegistrationRestController extends HerdBaseController
{
    public static final String BUSINESS_OBJECT_DATA_NOTIFICATIONS_URI_PREFIX = "/notificationRegistrations/businessObjectDataNotificationRegistrations";

    @Autowired
    private BusinessObjectDataNotificationRegistrationService businessObjectDataNotificationRegistrationService;

    /**
     * Creates a new business object data notification.
     *
     * @param request the information needed to create the business object data notification
     *
     * @return the created business object data notification
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_NOTIFICATIONS_URI_PREFIX, method = RequestMethod.POST, consumes = {"application/xml", "application/json"})
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_NOTIFICATION_REGISTRATIONS_POST)
    public BusinessObjectDataNotificationRegistration createBusinessObjectDataNotificationRegistration(
        @RequestBody BusinessObjectDataNotificationRegistrationCreateRequest request)
    {
        return businessObjectDataNotificationRegistrationService.createBusinessObjectDataNotificationRegistration(request);
    }

    /**
     * Updates an existing business object data notification by key.
     *
     * @param namespace the namespace
     * @param notificationName the business object data notification name
     * @param request the information needed to update the business object data notification
     *
     * @return the updated business object data notification
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_NOTIFICATIONS_URI_PREFIX + "/namespaces/{namespace}/notificationNames/{notificationName}",
        method = RequestMethod.PUT)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_NOTIFICATION_REGISTRATIONS_PUT)
    public BusinessObjectDataNotificationRegistration updateBusinessObjectDataNotificationRegistration(@PathVariable("namespace") String namespace,
        @PathVariable("notificationName") String notificationName, @RequestBody BusinessObjectDataNotificationRegistrationUpdateRequest request)
    {
        return businessObjectDataNotificationRegistrationService.updateBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(namespace,
            notificationName), request);
    }

    /**
     * Gets an existing business object data notification by key.
     *
     * @param namespace the namespace
     * @param notificationName the business object data notification name
     *
     * @return the retrieved business object data notification
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_NOTIFICATIONS_URI_PREFIX + "/namespaces/{namespace}/notificationNames/{notificationName}",
        method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_NOTIFICATION_REGISTRATIONS_GET)
    public BusinessObjectDataNotificationRegistration getBusinessObjectDataNotificationRegistration(@PathVariable("namespace") String namespace,
        @PathVariable("notificationName") String notificationName)
    {
        return businessObjectDataNotificationRegistrationService
            .getBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(namespace, notificationName));
    }

    /**
     * Deletes an existing business object data notification by key.
     *
     * @param namespace the namespace
     * @param notificationName the business object data notification name
     *
     * @return the business object data notification that got deleted
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_NOTIFICATIONS_URI_PREFIX + "/namespaces/{namespace}/notificationNames/{notificationName}",
        method = RequestMethod.DELETE)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_NOTIFICATION_REGISTRATIONS_DELETE)
    public BusinessObjectDataNotificationRegistration deleteBusinessObjectDataNotification(@PathVariable("namespace") String namespace,
        @PathVariable("notificationName") String notificationName)
    {
        return businessObjectDataNotificationRegistrationService
            .deleteBusinessObjectDataNotificationRegistration(new NotificationRegistrationKey(namespace, notificationName));
    }

    /**
     * Gets the list of business object data notifications that are defined in the system.
     *
     * @return the retrieved business object data notification list
     */
    @RequestMapping(value = BUSINESS_OBJECT_DATA_NOTIFICATIONS_URI_PREFIX + "/namespaces/{namespace}", method = RequestMethod.GET)
    @Secured(SecurityFunctions.FN_BUSINESS_OBJECT_DATA_NOTIFICATION_REGISTRATIONS_ALL_GET)
    public BusinessObjectDataNotificationRegistrationKeys getBusinessObjectDataNotificationRegistrations(@PathVariable("namespace") String namespace)
    {
        return businessObjectDataNotificationRegistrationService.getBusinessObjectDataNotificationRegistrations(namespace);
    }
}
