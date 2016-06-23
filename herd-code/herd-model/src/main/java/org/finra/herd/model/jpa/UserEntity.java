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

import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.Type;

/**
 * A user.
 */
@Table(name = UserEntity.TABLE_NAME)
@Immutable
@Entity
public class UserEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "user_tbl";

    /**
     * The user id column.
     */
    @Id
    @Column(name = "user_id")
    private String userId;

    /**
     * The namespace authorization admin column.
     */
    @Column(name = "name_space_athrn_admin_fl")
    @Type(type = "yes_no")
    private Boolean namespaceAuthorizationAdmin;

    public String getUserId()
    {
        return userId;
    }

    public void setUserId(String userId)
    {
        this.userId = userId;
    }

    public Boolean getNamespaceAuthorizationAdmin()
    {
        return namespaceAuthorizationAdmin;
    }

    public void setNamespaceAuthorizationAdmin(Boolean namespaceAuthorizationAdmin)
    {
        this.namespaceAuthorizationAdmin = namespaceAuthorizationAdmin;
    }
}
