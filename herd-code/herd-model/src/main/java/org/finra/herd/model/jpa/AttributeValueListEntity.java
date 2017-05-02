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

import java.util.Collection;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonManagedReference;

@Table(name = AttributeValueListEntity.TABLE_NAME)
@Entity
public class AttributeValueListEntity extends AuditableEntity
{

    public static final String TABLE_NAME = "atrbt_value_list";

    @Column(name = TABLE_NAME + "_nm")
    private String attributeValueListName;

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Integer id;

    @ManyToOne
    @JoinColumn(name = "name_space_cd", referencedColumnName = "name_space_cd", nullable = false)
    private NamespaceEntity namespace;

    @JsonManagedReference
    @OneToMany(mappedBy = "attributeValueListEntity", orphanRemoval = true, cascade = {CascadeType.ALL})
    @OrderBy("allowedAttributeValue")
    private Collection<AllowedAttributeValueEntity> allowedAttributeValues;

    public String getAttributeValueListName()
    {
        return attributeValueListName;
    }

    public void setAttributeValueListName(String attributeValueListName)
    {
        this.attributeValueListName = attributeValueListName;
    }

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
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

    public Collection<AllowedAttributeValueEntity> getAllowedAttributeValues()
    {
        return allowedAttributeValues;
    }

    public void setAllowedAttributeValues(Collection<AllowedAttributeValueEntity> allowedAttributeValues)
    {
        this.allowedAttributeValues = allowedAttributeValues;
    }
}
