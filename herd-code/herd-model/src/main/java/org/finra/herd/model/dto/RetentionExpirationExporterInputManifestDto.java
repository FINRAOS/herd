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

import org.finra.herd.model.api.xml.BusinessObjectDataKeys;

/**
 * An input manifest for the uploader.
 */
public class RetentionExpirationExporterInputManifestDto extends DataBridgeBaseManifestDto
{
    private BusinessObjectDataKeys businessObjectDataKeys = new BusinessObjectDataKeys();

    @Override
    public String getNamespace()
    {
        return namespace;
    }

    @Override
    public void setNamespace(String namespace)
    {
        this.namespace = namespace;
    }

    @Override
    public String getBusinessObjectDefinitionName()
    {
        return businessObjectDefinitionName;
    }

    @Override
    public void setBusinessObjectDefinitionName(String businessObjectDefinitionName)
    {
        this.businessObjectDefinitionName = businessObjectDefinitionName;
    }

    public BusinessObjectDataKeys getBusinessObjectDataKeys()
    {
        return businessObjectDataKeys;
    }

    public void setBusinessObjectDataKeys(BusinessObjectDataKeys businessObjectDataKeys)
    {
        this.businessObjectDataKeys = businessObjectDataKeys;
    }
}
