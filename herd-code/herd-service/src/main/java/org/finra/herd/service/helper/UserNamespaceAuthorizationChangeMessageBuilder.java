package org.finra.herd.service.helper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.NotificationMessage;

/**
 * The builder that knows how to build User Namespace Authorization Change notification messages
 */
@Component
public class UserNamespaceAuthorizationChangeMessageBuilder extends AbstractNotificationMessageBuilder
{
    /**
     * Builds a list of notification messages for the user namespace authorization change event. The result list might be empty if if no messages should be
     * sent.
     *
     * @param userNamespaceAuthorizationKey the key for the user namespace authorization object
     *
     * @return the list of user namespace authorization change notification messages
     */
    public List<NotificationMessage> buildMessages(UserNamespaceAuthorizationKey userNamespaceAuthorizationKey)
    {
        return buildNotificationMessages(new UserNamespaceAuthorizationChangeEvent(userNamespaceAuthorizationKey));
    }

    @Override
    public Map<String, Object> getNotificationMessageVelocityContextMap(NotificationEvent notificationEvent)
    {
        UserNamespaceAuthorizationChangeEvent event = (UserNamespaceAuthorizationChangeEvent) notificationEvent;

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

    /**
     * The User Namespace Authorization change event
     */
    public static class UserNamespaceAuthorizationChangeEvent extends NotificationEvent
    {
        private UserNamespaceAuthorizationKey userNamespaceAuthorizationKey;

        public UserNamespaceAuthorizationChangeEvent(UserNamespaceAuthorizationKey userNamespaceAuthorizationKey)
        {
            setMessageDefinitionKey(ConfigurationValue.HERD_NOTIFICATION_USER_NAMESPACE_AUTHORIZATION_CHANGE_MESSAGE_DEFINITIONS);
            setEventName("userNamespaceAuthorizationChangeEvent");
            this.userNamespaceAuthorizationKey = userNamespaceAuthorizationKey;
        }

        public UserNamespaceAuthorizationKey getUserNamespaceAuthorizationKey()
        {
            return userNamespaceAuthorizationKey;
        }

        public void setUserNamespaceAuthorizationKey(UserNamespaceAuthorizationKey userNamespaceAuthorizationKey)
        {
            this.userNamespaceAuthorizationKey = userNamespaceAuthorizationKey;
        }
    }
}
