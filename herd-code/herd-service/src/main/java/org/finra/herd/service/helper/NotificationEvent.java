package org.finra.herd.service.helper;

import org.finra.herd.model.dto.ConfigurationValue;

/**
 * The base class for all notification events
 */
public class NotificationEvent
{
    private ConfigurationValue messageDefinitionKey;
    private String eventName;

    /**
     * Get the message definition key for the notification message
     * @return
     */
    public ConfigurationValue getMessageDefinitionKey()
    {
        return messageDefinitionKey;
    }

    /**
     * Set the message definition key for the notification message
     * @param messageDefinitionKey
     */
    public void setMessageDefinitionKey(ConfigurationValue messageDefinitionKey)
    {
        this.messageDefinitionKey = messageDefinitionKey;
    }

    /**
     * Get the event name
     * @return event name
     */
    public String getEventName()
    {
        return eventName;
    }

    /**
     * Set the event name
     * @param eventName event name
     */
    public void setEventName(String eventName)
    {
        this.eventName = eventName;
    }
}
