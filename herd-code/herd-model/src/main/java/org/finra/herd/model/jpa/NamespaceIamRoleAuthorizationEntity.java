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

@Table
@Entity(name = NamespaceIamRoleAuthorizationEntity.TABLE_NAME)
public class NamespaceIamRoleAuthorizationEntity extends AuditableEntity
{
    protected static final String TABLE_NAME = "name_space_iam_role_athrn";

    private static final String SEQUENCE_NAME = TABLE_NAME + "_seq";

    @Id
    @GeneratedValue(generator = SEQUENCE_NAME)
    @SequenceGenerator(name = SEQUENCE_NAME, sequenceName = SEQUENCE_NAME, allocationSize = 1)
    @Column(name = "name_space_iam_role_athrn_id")
    private Long id;

    @ManyToOne
    @JoinColumn(name = "name_space_cd")
    private NamespaceEntity namespace;

    @Column(name = "iam_role_nm")
    private String iamRoleName;

    @Column(name = "athrn_ds")
    private String description;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public NamespaceEntity getNamespace()
    {
        return namespace;
    }

    public void setNamespace(NamespaceEntity namespace)
    {
        this.namespace = namespace;
    }

    public String getIamRoleName()
    {
        return iamRoleName;
    }

    public void setIamRoleName(String iamRoleName)
    {
        this.iamRoleName = iamRoleName;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }
}
