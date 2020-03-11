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
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

/**
 * A base entity class for notification action.
 */
@Table(name = NotificationActionEntity.TABLE_NAME)
@Entity
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "actn_type_cd", discriminatorType = DiscriminatorType.STRING)
public class NotificationActionEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "ntfcn_actn";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Long id;

    @ManyToOne
    @JoinColumn(name = "ntfcn_rgstn_id", referencedColumnName = "ntfcn_rgstn_id", nullable = false)
    private NotificationRegistrationEntity notificationRegistration;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public NotificationRegistrationEntity getNotificationRegistration()
    {
        return notificationRegistration;
    }

    public void setNotificationRegistration(NotificationRegistrationEntity notificationRegistration)
    {
        this.notificationRegistration = notificationRegistration;
    }
}
