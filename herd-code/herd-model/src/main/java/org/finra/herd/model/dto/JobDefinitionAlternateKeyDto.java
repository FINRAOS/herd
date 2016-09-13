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
package org.finra.herd.model.dto;

/**
 * A DTO that contains the alternate key fields for job definition.
 */
public class JobDefinitionAlternateKeyDto
{
    protected String jobName;

    protected String namespace;

    /**
     * Default no-arg constructor.
     */
    public JobDefinitionAlternateKeyDto()
    {
        super();
    }

    /**
     * Fully-initialising value constructor.
     *
     * @param namespace the namespace of the job definition
     * @param jobName the name of the job definition
     */
    public JobDefinitionAlternateKeyDto(final String namespace, final String jobName)
    {
        this.namespace = namespace;
        this.jobName = jobName;
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (!(object instanceof JobDefinitionAlternateKeyDto))
        {
            return false;
        }

        JobDefinitionAlternateKeyDto that = (JobDefinitionAlternateKeyDto) object;

        if (jobName != null ? !jobName.equals(that.jobName) : that.jobName != null)
        {
            return false;
        }
        if (namespace != null ? !namespace.equals(that.namespace) : that.namespace != null)
        {
            return false;
        }

        return true;
    }

    public String getJobName()
    {
        return jobName;
    }

    public void setJobName(String jobName)
    {
        this.jobName = jobName;
    }

    public String getNamespace()
    {
        return namespace;
    }

    public void setNamespace(String namespace)
    {
        this.namespace = namespace;
    }

    @Override
    public int hashCode()
    {
        int result = namespace != null ? namespace.hashCode() : 0;
        result = 31 * result + (jobName != null ? jobName.hashCode() : 0);
        return result;
    }
}
