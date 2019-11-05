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

import java.util.ArrayList;
import java.util.List;

import org.finra.herd.dao.helper.HerdDaoSecurityHelper;
import org.finra.herd.model.api.xml.MessageHeaderDefinition;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.MessageHeader;
import org.finra.herd.service.AbstractServiceTest;

/**
 * Test helper for various message builder tests
 */
public abstract class AbstractNotificationMessageBuilderTestHelper extends AbstractServiceTest
{
    protected static final String SUFFIX_ESCAPED_JSON = "\\\"\\\"<test>";

    protected static final String SUFFIX_ESCAPED_XML = "&quot;&quot;&lt;test&gt;";

    protected static final String SUFFIX_UNESCAPED = "\"\"<test>";

    /**
     * Returns a list of message header definitions.
     *
     * @return the list of message header definitions
     */
    protected List<MessageHeaderDefinition> getMessageHeaderDefinitions()
    {
        List<MessageHeaderDefinition> messageHeaderDefinitions = new ArrayList<>();

        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_ENVIRONMENT, "$herd_environment"));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_MESSAGE_TYPE, MESSAGE_TYPE));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_MESSAGE_VERSION, MESSAGE_VERSION));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_SOURCE_SYSTEM, SOURCE_SYSTEM));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_MESSAGE_ID, "$uuid"));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_USER_ID, "$username"));
        messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_NAMESPACE, "$namespace"));
        //messageHeaderDefinitions.add(new MessageHeaderDefinition(MESSAGE_HEADER_KEY_FILTER, "$namespace"));

        return messageHeaderDefinitions;
    }

    /**
     * Returns a list of expected message headers.
     *
     * @param expectedUuid the expected UUID
     *
     * @return the list of message headers
     */
    protected List<MessageHeader> getExpectedMessageHeaders(String expectedUuid)
    {
        List<MessageHeader> messageHeaders = new ArrayList<>();

        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_ENVIRONMENT, configurationHelper.getProperty(ConfigurationValue.HERD_ENVIRONMENT)));
        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_MESSAGE_TYPE, MESSAGE_TYPE));
        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_MESSAGE_VERSION, MESSAGE_VERSION));
        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_SOURCE_SYSTEM, SOURCE_SYSTEM));
        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_MESSAGE_ID, expectedUuid));
        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_USER_ID, HerdDaoSecurityHelper.SYSTEM_USER));
        messageHeaders.add(new MessageHeader(MESSAGE_HEADER_KEY_NAMESPACE, BDEF_NAMESPACE));

        return messageHeaders;
    }
}
