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
 * A DTO that contains the alternate key fields for EMR cluster.
 */
public class EmrClusterAlternateKeyDto
{
    /**
     * The namespace.
     */
    private String namespace;

    /**
     * The EMR cluster definition name.
     */
    private String emrClusterDefinitionName;

    /**
     * The EMR cluster name.
     */
    private String emrClusterName;

    public String getNamespace()
    {
        return namespace;
    }
    
    public void setNamespace(String namespace)
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
    
    public String getEmrClusterName()
    {
        return emrClusterName;
    }
    
    public void setEmrClusterName(String emrClusterName)
    {
        this.emrClusterName = emrClusterName;
    }

    /**
     * Returns a builder that can easily build this DTO.
     *
     * @return the builder.
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * A builder that makes it easier to construct this DTO.
     */
    public static class Builder
    {
        private EmrClusterAlternateKeyDto alternateKey = new EmrClusterAlternateKeyDto();

        public Builder namespace(String namespace)
        {
            alternateKey.setNamespace(namespace);
            return this;
        }

        public Builder emrClusterDefinitionName(String emrClusterDefinitionName)
        {
            alternateKey.setEmrClusterDefinitionName(emrClusterDefinitionName);
            return this;
        }

        public Builder emrClusterName(String emrClusterName)
        {
            alternateKey.setEmrClusterName(emrClusterName);
            return this;
        }

        public EmrClusterAlternateKeyDto build()
        {
            return alternateKey;
        }
    }
}
