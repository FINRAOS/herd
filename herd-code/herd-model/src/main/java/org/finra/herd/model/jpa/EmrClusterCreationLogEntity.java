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

@Entity
@Table(name = EmrClusterCreationLogEntity.TABLE_NAME)
public class EmrClusterCreationLogEntity extends AuditableEntity
{
    public static final String TABLE_NAME = "emr_clstr_crtn_log";

    @Id
    @GeneratedValue(generator = EmrClusterCreationLogEntity.TABLE_NAME + "_seq")
    @SequenceGenerator(name = EmrClusterCreationLogEntity.TABLE_NAME + "_seq", sequenceName = EmrClusterCreationLogEntity.TABLE_NAME + "_seq",
        allocationSize = 1)
    @Column(name = EmrClusterCreationLogEntity.TABLE_NAME + "_id")
    private Long id;

    @ManyToOne
    @JoinColumn(name = "name_space_cd", nullable = false)
    private NamespaceEntity namespace;

    @Column(name = "emr_clstr_dfntn_name_tx", nullable = false)
    private String emrClusterDefinitionName;

    @Column(name = "emr_clstr_id", nullable = false)
    private String emrClusterId;

    @Column(name = "emr_clstr_name_tx", nullable = false)
    private String emrClusterName;

    @Column(name = "emr_clstr_dfntn_cl", nullable = false)
    private String emrClusterDefinition;

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

    public String getEmrClusterDefinitionName()
    {
        return emrClusterDefinitionName;
    }

    public void setEmrClusterDefinitionName(String emrClusterDefinitionName)
    {
        this.emrClusterDefinitionName = emrClusterDefinitionName;
    }

    public String getEmrClusterId()
    {
        return emrClusterId;
    }

    public void setEmrClusterId(String emrClusterId)
    {
        this.emrClusterId = emrClusterId;
    }

    public String getEmrClusterName()
    {
        return emrClusterName;
    }

    public void setEmrClusterName(String emrClusterName)
    {
        this.emrClusterName = emrClusterName;
    }

    public String getEmrClusterDefinition()
    {
        return emrClusterDefinition;
    }

    public void setEmrClusterDefinition(String emrClusterDefinition)
    {
        this.emrClusterDefinition = emrClusterDefinition;
    }
}
