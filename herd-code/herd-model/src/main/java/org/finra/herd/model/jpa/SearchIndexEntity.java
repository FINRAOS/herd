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
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.Type;

/**
 * A search index.
 */
@Table(name = "srch_idx")
@Entity
public class SearchIndexEntity extends AuditableEntity
{
    /**
     * The search index name column.
     */
    @Id
    @Column(name = "srch_idx_nm")
    private String name;

    /**
     * The search index type column.
     */
    @ManyToOne
    @JoinColumn(name = "srch_idx_type_cd", referencedColumnName = "srch_idx_type_cd", nullable = false)
    private SearchIndexTypeEntity type;

    /**
     * The search index status column.
     */
    @ManyToOne
    @JoinColumn(name = "srch_idx_stts_cd", referencedColumnName = "srch_idx_stts_cd", nullable = false)
    private SearchIndexStatusEntity status;

    /**
     * The search index active column.
     */
    @Column(name = "actv_fl")
    @Type(type = "yes_no")
    private Boolean active;

    public Boolean getActive()
    {
        return active;
    }

    public void setActive(Boolean active)
    {
        this.active = active;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public SearchIndexTypeEntity getType()
    {
        return type;
    }

    public void setType(SearchIndexTypeEntity type)
    {
        this.type = type;
    }

    public SearchIndexStatusEntity getStatus()
    {
        return status;
    }

    public void setStatus(SearchIndexStatusEntity status)
    {
        this.status = status;
    }
}
