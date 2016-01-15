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
import javax.persistence.Id;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * A notification event type.
 */
@XmlRootElement
@XmlType
@Table(name = "ntfcn_event_type_cd_lk")
@Entity
public class NotificationEventTypeEntity extends AuditableEntity
{
    /**
     * Notification event type.
     */
    public static enum EventTypesBdata
    {
        BUS_OBJCT_DATA_RGSTN,
        BUS_OBJCT_DATA_STTS_CHG;
    };

    /**
     * The code column.
     */
    @Id
    @Column(name = "ntfcn_event_type_cd")
    private String code;

    @Column(name = "ntfcn_event_type_ds")
    private String description;

    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || getClass() != other.getClass())
        {
            return false;
        }

        NotificationEventTypeEntity notificationEventType = (NotificationEventTypeEntity) other;

        if (code != null ? !code.equals(notificationEventType.code) : notificationEventType.code != null)
        {
            return false;
        }
        if (description != null ? !description.equals(notificationEventType.description) : notificationEventType.description != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = code != null ? code.hashCode() : 0;
        result = 31 * result + (description != null ? description.hashCode() : 0);
        return result;
    }
}
