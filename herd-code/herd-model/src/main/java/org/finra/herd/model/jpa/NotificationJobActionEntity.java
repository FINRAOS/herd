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
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * A notification job action.
 */
@XmlRootElement
@XmlType
@Entity
@DiscriminatorValue(ActionTypeEntity.ACTION_TYPE_JOB)
public class NotificationJobActionEntity extends NotificationActionEntity
{
    /**
     * The job definition column.
     */
    @ManyToOne
    @JoinColumn(name = "job_dfntn_id", referencedColumnName = "job_dfntn_id", nullable = false)
    private JobDefinitionEntity jobDefinition;

    /**
     * The correlation data column.
     */
    @Column(name = "crltn_data_tx", length = 4000)
    private String correlationData;

    public JobDefinitionEntity getJobDefinition()
    {
        return jobDefinition;
    }

    public void setJobDefinition(JobDefinitionEntity jobDefinition)
    {
        this.jobDefinition = jobDefinition;
    }

    public String getCorrelationData()
    {
        return correlationData;
    }

    public void setCorrelationData(String correlationData)
    {
        this.correlationData = correlationData;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || getClass() != other.getClass())
        {
            return false;
        }
        if (!super.equals(other))
        {
            return false;
        }

        NotificationJobActionEntity that = (NotificationJobActionEntity) other;

        if (correlationData != null ? !correlationData.equals(that.correlationData) : that.correlationData != null)
        {
            return false;
        }
        if (jobDefinition != null ? !jobDefinition.equals(that.jobDefinition) : that.jobDefinition != null)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (jobDefinition != null ? jobDefinition.hashCode() : 0);
        result = 31 * result + (correlationData != null ? correlationData.hashCode() : 0);
        return result;
    }
}
