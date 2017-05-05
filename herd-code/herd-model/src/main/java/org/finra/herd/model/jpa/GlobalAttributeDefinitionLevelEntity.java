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

/**
 * A global attribute definition level
 */
@Table(name = GlobalAttributeDefinitionLevelEntity.TABLE_NAME)
@Entity
public class GlobalAttributeDefinitionLevelEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "glbl_atrbt_dfntn_level_cd_lk";

    /**
     * The global attribute definition level.
     */
    @Id
    @Column(name = "glbl_atrbt_dfntn_level_cd")
    private String globalAttributeDefinitionLevel;

    public String getGlobalAttributeDefinitionLevel()
    {
        return globalAttributeDefinitionLevel;
    }

    public void setGlobalAttributeDefinitionLevel(String globalAttributeDefinitionLevel)
    {
        this.globalAttributeDefinitionLevel = globalAttributeDefinitionLevel;
    }

    /**
     * Supported levels.
     */
    public static enum GlobalAttributeDefinitionLevels
    {
        BUS_OBJCT_FRMT
    }
}
