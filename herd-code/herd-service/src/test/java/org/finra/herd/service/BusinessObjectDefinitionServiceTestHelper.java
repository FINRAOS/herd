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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.DataProviderDaoTestHelper;
import org.finra.herd.dao.NamespaceDaoTestHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.SampleDataFile;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;

@Component
public class BusinessObjectDefinitionServiceTestHelper
{
    @Autowired
    private DataProviderDaoTestHelper dataProviderDaoTestHelper;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    /**
     * Create and persist database entities required for testing.
     */
    public void createDatabaseEntitiesForBusinessObjectDefinitionTesting()
    {
        createDatabaseEntitiesForBusinessObjectDefinitionTesting(AbstractServiceTest.NAMESPACE, AbstractServiceTest.DATA_PROVIDER_NAME);
    }

    /**
     * Create and persist database entities required for testing.
     *
     * @param namespaceCode the namespace code
     * @param dataProviderName the data provider name
     */
    public void createDatabaseEntitiesForBusinessObjectDefinitionTesting(String namespaceCode, String dataProviderName)
    {
        // Create a namespace entity.
        namespaceDaoTestHelper.createNamespaceEntity(namespaceCode);

        // Create a data provider entity.
        dataProviderDaoTestHelper.createDataProviderEntity(dataProviderName);
    }

    /**
     * Creates a business object definition from a business object definition entity.
     *
     * @param businessObjectDefinitionEntity the specified business object definition entity
     *
     * @return the business object definition entity
     */
    public BusinessObjectDefinition createBusinessObjectDefinitionFromEntityForSearchTesting(BusinessObjectDefinitionEntity businessObjectDefinitionEntity)
    {
        BusinessObjectDefinition businessObjectDefinition = new BusinessObjectDefinition();

        businessObjectDefinition.setNamespace(businessObjectDefinitionEntity.getNamespace().getCode());
        businessObjectDefinition.setBusinessObjectDefinitionName(businessObjectDefinitionEntity.getName());
        businessObjectDefinition.setDataProviderName(businessObjectDefinitionEntity.getDataProvider().getName());
        businessObjectDefinition.setDisplayName(businessObjectDefinitionEntity.getDisplayName());
        businessObjectDefinition.setShortDescription(StringUtils.left(businessObjectDefinitionEntity.getDescription(),
            configurationHelper.getProperty(ConfigurationValue.SHORT_DESCRIPTION_LENGTH, Integer.class)));

        return businessObjectDefinition;
    }

    /**
     * Returns an expected string representation of the specified business object definition key.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     *
     * @return the string representation of the specified business object definition key
     */
    public String getExpectedBusinessObjectDefinitionKeyAsString(String namespace, String businessObjectDefinitionName)
    {
        return String.format("namespace: \"%s\", businessObjectDefinitionName: \"%s\"", namespace, businessObjectDefinitionName);
    }

    /**
     * Returns the business object definition not found error message per specified parameters.
     *
     * @param namespace the namespace of the business object definition
     * @param businessObjectDefinitionName the name of the business object definition
     *
     * @return the business object definition not found error message
     */
    public String getExpectedBusinessObjectDefinitionNotFoundErrorMessage(String namespace, String businessObjectDefinitionName)
    {
        return String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".", businessObjectDefinitionName, namespace);
    }

    /**
     * Gets a new list of attributes.
     *
     * @return the list of attributes.
     */
    public List<Attribute> getNewAttributes()
    {
        List<Attribute> attributes = new ArrayList<>();

        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_2_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_2));
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_3_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_3));

        return attributes;
    }

    /**
     * Gets a second set of test attributes.
     *
     * @return the list of attributes
     */
    public List<Attribute> getNewAttributes2()
    {
        List<Attribute> attributes = new ArrayList<>();

        // Attribute 1 has a new value compared to the first set of test attributes.
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_1_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_1_UPDATED));

        // Attribute 2 is missing compared to the first set of the test attributes.

        // Attribute 3 is identical to the one from the first set of the test attributes.
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_3_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_3));

        // Attribute 4 is not present in the first set of the test attributes.
        attributes.add(new Attribute(AbstractServiceTest.ATTRIBUTE_NAME_4_MIXED_CASE, AbstractServiceTest.ATTRIBUTE_VALUE_4));

        return attributes;
    }

    /**
     * Gets a list of test sample data files.
     *
     * @return the list of sample data files
     */
    public List<SampleDataFile> getTestSampleDataFiles()
    {
        List<SampleDataFile> sampleDataFiles = new ArrayList<>();

        sampleDataFiles.add(new SampleDataFile(AbstractServiceTest.DIRECTORY_PATH, AbstractServiceTest.FILE_NAME));
        sampleDataFiles.add(new SampleDataFile(AbstractServiceTest.DIRECTORY_PATH, AbstractServiceTest.FILE_NAME_2));

        return sampleDataFiles;
    }

    /**
     * Validates a list of Attributes against the expected values.
     *
     * @param expectedAttributes the list of expected Attributes
     * @param actualAttributes the list of actual Attributes to be validated
     */
    public void validateAttributes(List<Attribute> expectedAttributes, List<Attribute> actualAttributes)
    {
        assertEquals(expectedAttributes.size(), actualAttributes.size());
        for (int i = 0; i < expectedAttributes.size(); i++)
        {
            Attribute expectedAttribute = expectedAttributes.get(i);
            Attribute actualAttribute = actualAttributes.get(i);
            assertEquals(expectedAttribute.getName(), actualAttribute.getName());
            assertEquals(expectedAttribute.getValue(), actualAttribute.getValue());
        }
    }
}
