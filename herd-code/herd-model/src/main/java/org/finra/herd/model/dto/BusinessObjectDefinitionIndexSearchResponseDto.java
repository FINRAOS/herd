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
 * BusinessObjectDefinitionIndexSearchResponseDto
 * <p>
 * "_source": { "displayName": null, "name": "SCHEMA_TEST", "namespace": { "code": "NODE_TEST" }, "description": "Shepherd tests definition." "dataProvider": {
 * "name": "NODE_JS" } }
 */
public class BusinessObjectDefinitionIndexSearchResponseDto
{
    private DataProvider dataProvider;

    private String description;

    private String displayName;

    private String name;

    private Namespace namespace;

    /**
     * Empty constructor needed for JSON object mapping.
     */
    public BusinessObjectDefinitionIndexSearchResponseDto()
    {
        // Empty constructor needed for JSON object mapping
    }

    /**
     * Constructor for DTO.
     *
     * @param dataProviderName the data provider name string
     * @param description the description string
     * @param displayName the display name string
     * @param name the name string
     * @param namespaceCode the namespace code string
     */
    public BusinessObjectDefinitionIndexSearchResponseDto(String dataProviderName, String description, String displayName, String name, String namespaceCode)
    {
        dataProvider = new DataProvider();
        dataProvider.setName(dataProviderName);
        this.description = description;
        this.displayName = displayName;
        this.name = name;
        namespace = new Namespace();
        namespace.setCode(namespaceCode);
    }

    public DataProvider getDataProvider()
    {
        return dataProvider;
    }

    public void setDataProvider(DataProvider dataProvider)
    {
        this.dataProvider = dataProvider;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public String getDisplayName()
    {
        return displayName;
    }

    public void setDisplayName(String displayName)
    {
        this.displayName = displayName;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public Namespace getNamespace()
    {
        return namespace;
    }

    public void setNamespace(Namespace namespace)
    {
        this.namespace = namespace;
    }

    public static class DataProvider
    {
        private String name;

        public String getName()
        {
            return name;
        }

        public void setName(String name)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return "DataProvider{" +
                "name='" + name + '\'' +
                '}';
        }
    }

    public static class Namespace
    {
        private String code;

        public String getCode()
        {
            return code;
        }

        public void setCode(String code)
        {
            this.code = code;
        }

        @Override
        public String toString()
        {
            return "Namespace{" +
                "code='" + code + '\'' +
                '}';
        }
    }

    @Override
    public String toString()
    {
        return "BusinessObjectDefinitionIndexSearchResponseDto{" +
            "dataProvider=" + dataProvider +
            ", description='" + description + '\'' +
            ", displayName='" + displayName + '\'' +
            ", name='" + name + '\'' +
            ", namespace=" + namespace +
            '}';
    }
}


