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

import org.finra.herd.model.api.xml.NotificationRegistrationStatusUpdateRequest;
import org.finra.herd.model.api.xml.NotificationRegistrationStatusUpdateResponse;
import org.finra.herd.model.dto.SecurityFunctions;
import org.finra.herd.service.NotificationRegistrationStatusService;
import org.finra.herd.ui.constants.UiConstants;

/**
 * The REST controller for notification registration statuses.
 */
@RestController
@RequestMapping(value = UiConstants.REST_URL_BASE, produces = {"application/xml", "application/json"})
@Api(tags = "Notification Registration Status")
public class NotificationRegistrationStatusRestController extends HerdBaseController
{
    @Autowired
    private NotificationRegistrationStatusService notificationRegistrationStatusService;

    /**
     * Updates the status of a notification registration.
     * <p>Requires WRITE permission on namespace</p>
     * 
     * @param namespace The namespace of the notification registration.
     * @param notificationName The name of the notification registration.
     * @param notificationRegistrationStatusUpdateRequest The request to update the status.
     * @return The response of the update request.
     */
    @Secured(SecurityFunctions.FN_NOTIFICATION_REGISTRATION_STATUS_PUT)
    @RequestMapping(value = "/notificationRegistrationStatus/namespaces/{namespace}/notificationNames/{notificationName}", method = RequestMethod.PUT,
        consumes = {"application/xml", "application/json"})
    public NotificationRegistrationStatusUpdateResponse updateNotificationRegistrationStatus(@PathVariable("namespace") String namespace,
        @PathVariable("notificationName") String notificationName,
        @RequestBody NotificationRegistrationStatusUpdateRequest notificationRegistrationStatusUpdateRequest)
    {
        return notificationRegistrationStatusService.updateNotificationRegistrationStatus(namespace, notificationName,
            notificationRegistrationStatusUpdateRequest);
    }
}
