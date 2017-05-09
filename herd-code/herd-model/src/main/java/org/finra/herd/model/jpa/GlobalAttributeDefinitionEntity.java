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

/**
 * A global attribute definition entity.
 */
@Table(name = GlobalAttributeDefinitionEntity.TABLE_NAME)
@Entity
public class GlobalAttributeDefinitionEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "glbl_atrbt_dfntn";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Integer id;

    /**
     * The global attribute definition level.
     */
    @ManyToOne
    @JoinColumn(name = "glbl_atrbt_dfntn_level_cd", referencedColumnName = "glbl_atrbt_dfntn_level_cd", nullable = false)
    private GlobalAttributeDefinitionLevelEntity globalAttributeDefinitionLevel;

    /**
     * The attribute value list.
     */
    @ManyToOne
    @JoinColumn(name = "atrbt_value_list_id", referencedColumnName = "atrbt_value_list_id", nullable = true)
    private AttributeValueListEntity attributeValueListEntity;

    /**
     * The  global attribute definition name.
     */
    @Column(name = TABLE_NAME + "_name_tx", nullable = false)
    private String globalAttributeDefinitionName;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public GlobalAttributeDefinitionLevelEntity getGlobalAttributeDefinitionLevel()
    {
        return globalAttributeDefinitionLevel;
    }

    public void setGlobalAttributeDefinitionLevel(GlobalAttributeDefinitionLevelEntity globalAttributeDefinitionLevel)
    {
        this.globalAttributeDefinitionLevel = globalAttributeDefinitionLevel;
    }

    public String getGlobalAttributeDefinitionName()
    {
        return globalAttributeDefinitionName;
    }

    public void setGlobalAttributeDefinitionName(String globalAttributeDefinitionName)
    {
        this.globalAttributeDefinitionName = globalAttributeDefinitionName;
    }
    
    public AttributeValueListEntity getAttributeValueListEntity()
    {
        return attributeValueListEntity;
    }

    public void setAttributeValueListEntity(AttributeValueListEntity attributeValueListEntity)
    {
        this.attributeValueListEntity = attributeValueListEntity;
    }
}
