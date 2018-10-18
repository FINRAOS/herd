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
package org.finra.herd.service.helper.notification;

import java.util.HashMap;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.dto.NotificationEvent;
import org.finra.herd.model.dto.UserNamespaceAuthorizationChangeNotificationEvent;
import org.finra.herd.service.helper.notification.AbstractNotificationMessageBuilder;
import org.finra.herd.service.helper.notification.NotificationMessageBuilder;

/**
 * The builder that knows how to build User Namespace Authorization Change notification messages
 */
@Component
public class UserNamespaceAuthorizationChangeMessageBuilder extends AbstractNotificationMessageBuilder implements NotificationMessageBuilder
{
    @Override
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification =
        "The NotificationEvent is cast to a UserNamespaceAuthorizationChangeNotificationEvent which is always the case since" +
            " we manage the event type to a builder in a map defined in NotificationMessageManager")
    public Map<String, Object> getNotificationMessageVelocityContextMap(NotificationEvent notificationEvent)
    {
        UserNamespaceAuthorizationChangeNotificationEvent event = (UserNamespaceAuthorizationChangeNotificationEvent) notificationEvent;

        Map<String, Object> velocityContextMap = new HashMap<>();

        addObjectPropertyToContext(velocityContextMap, "userNamespaceAuthorizationKey", event.getUserNamespaceAuthorizationKey(),
            new UserNamespaceAuthorizationKey(escapeJson(event.getUserNamespaceAuthorizationKey().getUserId()),
                escapeJson(event.getUserNamespaceAuthorizationKey().getNamespace())),
            new UserNamespaceAuthorizationKey(escapeXml(event.getUserNamespaceAuthorizationKey().getUserId()),
                escapeXml(event.getUserNamespaceAuthorizationKey().getNamespace())));

        // Add the namespace Velocity property for the header.
        addStringPropertyToContext(velocityContextMap, "namespace", event.getUserNamespaceAuthorizationKey().getNamespace());

        return velocityContextMap;
    }

}
