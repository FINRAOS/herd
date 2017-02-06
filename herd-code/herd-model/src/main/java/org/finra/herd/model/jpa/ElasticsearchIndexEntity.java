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

/**
 * An Elasticsearch index.
 */
@Table(name = "elastic_srch_idx")
@Entity
public class ElasticsearchIndexEntity extends AuditableEntity
{
    /**
     * The Elasticsearch index name column.
     */
    @Id
    @Column(name = "elastic_srch_idx_nm")
    private String name;

    /**
     * The Elasticsearch index type column.
     */
    @ManyToOne
    @JoinColumn(name = "elastic_srch_idx_type_cd", referencedColumnName = "elastic_srch_idx_type_cd", nullable = false)
    private ElasticsearchIndexTypeEntity type;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public ElasticsearchIndexTypeEntity getType()
    {
        return type;
    }

    public void setType(ElasticsearchIndexTypeEntity type)
    {
        this.type = type;
    }
}
