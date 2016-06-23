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
package org.finra.herd.service.helper;

import java.util.LinkedList;
import java.util.Queue;

import org.springframework.stereotype.Component;

import org.finra.herd.model.dto.JmsMessage;

/**
 * An in-memory JMS message queue.
 */
@Component
public class JmsMessageInMemoryQueue
{
    private static final ThreadLocal<Queue<JmsMessage>> QUEUE = new ThreadLocal<Queue<JmsMessage>>()
    {
        @Override
        protected Queue<JmsMessage> initialValue()
        {
            return new LinkedList<>();
        }
    };

    /**
     * Adds a JMS message to the queue.
     *
     * @param jmsMessage the JMS message to be added
     */
    public void add(JmsMessage jmsMessage)
    {
        QUEUE.get().add(jmsMessage);
    }

    /**
     * Removes all of the elements from the queue.
     */
    public void clear()
    {
        QUEUE.get().clear();
    }

    /**
     * Returns <tt>true</tt> if the queue contains no elements.
     *
     * @return <tt>true</tt> if the queue contains no elements
     */
    public boolean isEmpty()
    {
        return QUEUE.get().isEmpty();
    }

    /**
     * Removes a JMS message from the head of the queue.
     *
     * @return the JMS message removed from the head of the queue
     * @throws java.util.NoSuchElementException if the queue is empty
     */
    public JmsMessage remove()
    {
        return QUEUE.get().remove();
    }

    /**
     * Returns the number of elements in the queue.
     *
     * @return the number of elements in the queue
     */
    public int size()
    {
        return QUEUE.get().size();
    }
}
