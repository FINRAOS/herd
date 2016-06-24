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
package org.finra.herd.model.dto;

/**
 * A DTO that holds various fields required to publish a JMS message.
 */
public class JmsMessage
{
    /**
     * Default no-arg constructor.
     */
    public JmsMessage()
    {
        super();
    }

    /**
     * Fully-initialising value constructor.
     *
     * @param jmsQueueName the JMS queue name
     * @param messageText the contents of the message. String maximum 256 KB in size
     */
    public JmsMessage(final String jmsQueueName, final String messageText)
    {
        this.jmsQueueName = jmsQueueName;
        this.messageText = messageText;
    }

    /**
     * The JMS queue name.
     */
    private String jmsQueueName;

    /**
     * The contents of the message. String maximum 256 KB in size.
     */
    private String messageText;

    public String getJmsQueueName()
    {
        return jmsQueueName;
    }

    public void setJmsQueueName(String jmsQueueName)
    {
        this.jmsQueueName = jmsQueueName;
    }

    public String getMessageText()
    {
        return messageText;
    }

    public void setMessageText(String messageText)
    {
        this.messageText = messageText;
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (!(object instanceof JmsMessage))
        {
            return false;
        }

        JmsMessage that = (JmsMessage) object;

        if (jmsQueueName != null ? !jmsQueueName.equals(that.jmsQueueName) : that.jmsQueueName != null)
        {
            return false;
        }
        if (messageText != null ? !messageText.equals(that.messageText) : that.messageText != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = jmsQueueName != null ? jmsQueueName.hashCode() : 0;
        result = 31 * result + (messageText != null ? messageText.hashCode() : 0);
        return result;
    }
}
