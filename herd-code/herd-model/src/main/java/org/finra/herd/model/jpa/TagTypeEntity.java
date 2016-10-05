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
 * A tag type.
 */
@Table(name = TagTypeEntity.TABLE_NAME)
@Entity
public class TagTypeEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "tag_type";

    /**
     * The tag type code column.
     */
    @Id
    @Column(name = "tag_type_cd", nullable = false)
    private String code;

    /**
     * The display name column.
     */
    @Column(name = "dsply_name_tx", nullable = false)
    private String displayName;

    /**
     * The order number.
     */
    @Column(name = "pstn_nb", nullable = false)
    private Integer orderNumber;

    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
    }

    public String getDisplayName()
    {
        return displayName;
    }

    public void setDisplayName(String displayName)
    {
        this.displayName = displayName;
    }

    public Integer getOrderNumber()
    {
        return orderNumber;
    }

    public void setOrderNumber(Integer orderNumber)
    {
        this.orderNumber = orderNumber;
    }
}
