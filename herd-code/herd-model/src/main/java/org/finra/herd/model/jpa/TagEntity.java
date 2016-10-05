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
 * A tag entity.
 */
@Table(name = TagEntity.TABLE_NAME)
@Entity
public class TagEntity extends AuditableEntity
{
    /**
     * The table name.
     */
    public static final String TABLE_NAME = "tag";

    @Id
    @Column(name = TABLE_NAME + "_id")
    @GeneratedValue(generator = TABLE_NAME + "_seq")
    @SequenceGenerator(name = TABLE_NAME + "_seq", sequenceName = TABLE_NAME + "_seq", allocationSize = 1)
    private Integer id;

    /**
     * The tag code column.
     */
    @Column(name = TABLE_NAME + "_cd", nullable = false)
    private String tagCode;

    /**
     * The parent tagType.
     */
    @ManyToOne
    @JoinColumn(name = "tag_type_cd", referencedColumnName = "tag_type_cd", nullable = false)
    private TagTypeEntity tagType;

    /**
     * The tag's display name.
     */
    @Column(name = "dsply_name_tx", nullable = true)
    private String displayName;

    /**
     * The tag description column
     */
    @Column(name = "desc_tx", nullable = true)
    private String description;

    public Integer getId()
    {
        return id;
    }

    public void setId(Integer id)
    {
        this.id = id;
    }

    public String getTagCode()
    {
        return tagCode;
    }

    public void setTagCode(String tagCode)
    {
        this.tagCode = tagCode;
    }

    public TagTypeEntity getTagType()
    {
        return tagType;
    }

    public void setTagType(TagTypeEntity tagType)
    {
        this.tagType = tagType;
    }

    public String getDisplayName()
    {
        return displayName;
    }

    public void setDisplayName(String displayName)
    {
        this.displayName = displayName;
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
