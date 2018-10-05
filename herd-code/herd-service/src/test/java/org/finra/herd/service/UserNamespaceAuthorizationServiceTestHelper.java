package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.springframework.stereotype.Component;

import org.finra.herd.model.api.xml.UserNamespaceAuthorizationKey;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.model.dto.NotificationMessage;

@Component
public class UserNamespaceAuthorizationServiceTestHelper
{
    /**
     * Validates a user namespace authorization change notification message.
     *
     * @param expectedMessageType the expected message type
     * @param expectedMessageDestination the expected message destination
     * @param expectedUsername the expected user name that triggered the message
     * @param expectedUserNamespaceAuthorizationKey the expected user namespace authorization key
     * @param expectedMessageHeaders the list of expected message headers
     * @param notificationMessage the notification message to be validated
     */
    public void validateUserNamespaceAuthorizationChangeMessageWithXmlPayload(String expectedMessageType, String expectedMessageDestination,
        String expectedUsername, UserNamespaceAuthorizationKey expectedUserNamespaceAuthorizationKey, List<MessageHeader> expectedMessageHeaders,
        NotificationMessage notificationMessage)
    {
        assertNotNull(notificationMessage);

        assertEquals(expectedMessageType, notificationMessage.getMessageType());
        assertEquals(expectedMessageDestination, notificationMessage.getMessageDestination());

        String messageText = notificationMessage.getMessageText();

        validateXmlFieldPresent(messageText, "triggered-by-username", expectedUsername);
        validateXmlFieldPresent(messageText, "context-message-type", "testDomain/testApplication/UserNamespaceAuthorizationChanged");

        validateXmlFieldPresent(messageText, "namespace", expectedUserNamespaceAuthorizationKey.getNamespace());
        validateXmlFieldPresent(messageText, "userId", expectedUserNamespaceAuthorizationKey.getUserId());

        assertEquals(expectedMessageHeaders, notificationMessage.getMessageHeaders());
    }

    /**
     * Validates that a specified XML tag and value are present in the message.
     *
     * @param message the XML message.
     * @param xmlTagName the XML tag name (without the '<', '/', and '>' characters).
     * @param value the value of the data for the tag.
     */
    private void validateXmlFieldPresent(String message, String xmlTagName, Object value)
    {
        assertTrue(xmlTagName + " \"" + value + "\" expected, but not found.",
            message.contains("<" + xmlTagName + ">" + (value == null ? null : value.toString()) + "</" + xmlTagName + ">"));
    }
}
