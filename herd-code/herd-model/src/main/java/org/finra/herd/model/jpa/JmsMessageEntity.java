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
package org.finra.herd.model.jpa;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

/**
 * An instance of JMS message.
 */
@Table(name = JmsMessageEntity.TABLE_NAME)
@Entity
public class JmsMessageEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "jms_msg";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Integer id;

    /**
     * The JMS queue name column.
     */
    @Column(name = "jms_queue_nm", nullable = false)
    private String jmsQueueName;

    /**
     * The message text column.
     */
    @Column(name = "msg_tx")
    private String messageText;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

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
}
