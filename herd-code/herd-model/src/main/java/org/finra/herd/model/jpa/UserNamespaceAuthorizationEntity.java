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
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import org.hibernate.annotations.Type;

/**
 * A user namespace authorization.
 */
@Table(name = UserNamespaceAuthorizationEntity.TABLE_NAME)
@Entity
public class UserNamespaceAuthorizationEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "user_name_space_athrn";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Long id;

    /**
     * The user id column.
     */
    @Column(name = "user_id")
    private String userId;

    /**
     * The namespace column.
     */
    @ManyToOne
    @JoinColumn(name = "name_space_cd", referencedColumnName = "name_space_cd", nullable = false)
    private NamespaceEntity namespace;

    /**
     * The read permission column.
     */
    @Column(name = "read_prmsn_fl")
    @Type(type = "yes_no")
    private Boolean readPermission;

    /**
     * The write permission column.
     */
    @Column(name = "write_prmsn_fl")
    @Type(type = "yes_no")
    private Boolean writePermission;

    /**
     * The execute permission column.
     */
    @Column(name = "exct_prmsn_fl")
    @Type(type = "yes_no")
    private Boolean executePermission;

    /**
     * The grant permission column.
     */
    @Column(name = "grant_prmsn_fl")
    @Type(type = "yes_no")
    private Boolean grantPermission;

    /**
     * The write descriptive content permission column.
     */
    @Column(name = "write_desc_content_prmsn_fl")
    @Type(type = "yes_no")
    private Boolean writeDescriptiveContentPermission;

    /**
     * The write attribute permission column.
     */
    @Column(name = "write_atrbt_prmsn_fl")
    @Type(type = "yes_no")
    private Boolean writeAttributePermission;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public String getUserId()
    {
        return userId;
    }

    public void setUserId(String userId)
    {
        this.userId = userId;
    }

    public NamespaceEntity getNamespace()
    {
        return namespace;
    }

    public void setNamespace(NamespaceEntity namespace)
    {
        this.namespace = namespace;
    }

    public Boolean getReadPermission()
    {
        return readPermission;
    }

    public void setReadPermission(Boolean readPermission)
    {
        this.readPermission = readPermission;
    }

    public Boolean getWritePermission()
    {
        return writePermission;
    }

    public void setWritePermission(Boolean writePermission)
    {
        this.writePermission = writePermission;
    }

    public Boolean getExecutePermission()
    {
        return executePermission;
    }

    public void setExecutePermission(Boolean executePermission)
    {
        this.executePermission = executePermission;
    }

    public Boolean getGrantPermission()
    {
        return grantPermission;
    }

    public void setGrantPermission(Boolean grantPermission)
    {
        this.grantPermission = grantPermission;
    }

    public Boolean getWriteDescriptiveContentPermission()
    {
        return writeDescriptiveContentPermission;
    }

    public void setWriteDescriptiveContentPermission(Boolean writeDescriptiveContentPermission)
    {
        this.writeDescriptiveContentPermission = writeDescriptiveContentPermission;
    }

    public Boolean getWriteAttributePermission()
    {
        return writeAttributePermission;
    }

    public void setWriteAttributePermission(Boolean writeAttributePermission)
    {
        this.writeAttributePermission = writeAttributePermission;
    }
}
