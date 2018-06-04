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
 * A business object definition description suggestion.
 */
@Table(name = BusinessObjectDefinitionDescriptionSuggestionEntity.TABLE_NAME)
@Entity
public class BusinessObjectDefinitionDescriptionSuggestionEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "bus_objct_dfntn_dscr_sgstn";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Integer id;

    /**
     * The business object definition entity.
     */
    @ManyToOne
    @JoinColumn(name = "bus_objct_dfntn_id", referencedColumnName = "bus_objct_dfntn_id", nullable = false)
    private BusinessObjectDefinitionEntity businessObjectDefinition;

    /**
     * The user id column.
     */
    @Column(name = "user_id", nullable = false)
    private String userId;

    /**
     * The description suggestion column.
     */
    @Column(name = "dscr_sgstn_tx", nullable = false)
    private String descriptionSuggestion;

    /**
     * The business object definition description suggestion status column.
     */
    @ManyToOne
    @JoinColumn(name = "bus_objct_dfntn_dscr_sgstn_stts_cd", referencedColumnName = "bus_objct_dfntn_dscr_sgstn_stts_cd", nullable = false)
    private BusinessObjectDefinitionDescriptionSuggestionStatusEntity status;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public BusinessObjectDefinitionEntity getBusinessObjectDefinition()
    {
        return businessObjectDefinition;
    }

    public void setBusinessObjectDefinition(BusinessObjectDefinitionEntity businessObjectDefinition)
    {
        this.businessObjectDefinition = businessObjectDefinition;
    }

    public String getUserId()
    {
        return userId;
    }

    public void setUserId(String userId)
    {
        this.userId = userId;
    }

    public String getDescriptionSuggestion()
    {
        return descriptionSuggestion;
    }

    public void setDescriptionSuggestion(String descriptionSuggestion)
    {
        this.descriptionSuggestion = descriptionSuggestion;
    }

    public BusinessObjectDefinitionDescriptionSuggestionStatusEntity getStatus()
    {
        return status;
    }

    public void setStatus(BusinessObjectDefinitionDescriptionSuggestionStatusEntity status)
    {
        this.status = status;
    }
}
