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
 * A class to identify a storage priority level. We have several priority levels for the storage policies that are based on business object definition, business
 * object format usage, and/or business object format file type being specified or not in the relative storage policy filter.
 */
public class StoragePolicyPriorityLevel
{
    /**
     * Default no-arg constructor.
     */
    public StoragePolicyPriorityLevel()
    {
        super();
    }

    /**
     * Fully-initialising value constructor.
     *
     * @param businessObjectDefinitionIsNull specifies whether or not the storage policy filter does not have a business object definition
     * @param usageIsNull specifies whether or not the storage policy filter does not have a business object format usage
     * @param fileTypeIsNull specifies whether or not the storage policy filter does not have a business object format file type
     */
    public StoragePolicyPriorityLevel(final boolean businessObjectDefinitionIsNull, final boolean usageIsNull, final boolean fileTypeIsNull)
    {
        this.businessObjectDefinitionIsNull = businessObjectDefinitionIsNull;
        this.usageIsNull = usageIsNull;
        this.fileTypeIsNull = fileTypeIsNull;
    }

    /**
     * Specifies whether or not the storage policy filter does not have a business object definition.
     */
    private boolean businessObjectDefinitionIsNull;

    /**
     * Specifies whether or not the storage policy filter does not have a business object format usage.
     */
    private boolean usageIsNull;

    /**
     * Specifies whether or not the storage policy filter does not have a business object format file type.
     */
    private boolean fileTypeIsNull;

    public boolean isBusinessObjectDefinitionIsNull()
    {
        return businessObjectDefinitionIsNull;
    }

    public void setBusinessObjectDefinitionIsNull(boolean businessObjectDefinitionIsNull)
    {
        this.businessObjectDefinitionIsNull = businessObjectDefinitionIsNull;
    }

    public boolean isUsageIsNull()
    {
        return usageIsNull;
    }

    public void setUsageIsNull(boolean usageIsNull)
    {
        this.usageIsNull = usageIsNull;
    }

    public boolean isFileTypeIsNull()
    {
        return fileTypeIsNull;
    }

    public void setFileTypeIsNull(boolean fileTypeIsNull)
    {
        this.fileTypeIsNull = fileTypeIsNull;
    }
}
