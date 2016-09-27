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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.dao.DataProviderDaoTestHelper;
import org.finra.herd.dao.NamespaceDaoTestHelper;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptiveInfoUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionUpdateRequest;

@Component
public class BusinessObjectDefinitionServiceTestHelper
{
    @Autowired
    private DataProviderDaoTestHelper dataProviderDaoTestHelper;

    @Autowired
    private NamespaceDaoTestHelper namespaceDaoTestHelper;

    /**
     * Creates a business object data definition create request.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param dataProviderName the data provider name
     * @param businessObjectDefinitionDescription the description of the business object definition
     * @param displayName the display name of the business object definition
     *
     * @return the newly created business object definition create request
     */
    public BusinessObjectDefinitionCreateRequest createBusinessObjectDefinitionCreateRequest(String namespaceCode, String businessObjectDefinitionName,
        String dataProviderName, String businessObjectDefinitionDescription, String displayName)
    {
        return createBusinessObjectDefinitionCreateRequest(namespaceCode, businessObjectDefinitionName, dataProviderName, businessObjectDefinitionDescription,
            displayName, AbstractServiceTest.NO_ATTRIBUTES);
    }

    /**
     * Creates a business object data definition create request.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param dataProviderName the data provider name
     * @param businessObjectDefinitionDescription the description of the business object definition
     * @param displayName the display name of the business object definition
     * @param attributes the list of attributes
     *
     * @return the newly created business object definition create request
     */
    public BusinessObjectDefinitionCreateRequest createBusinessObjectDefinitionCreateRequest(String namespaceCode, String businessObjectDefinitionName,
        String dataProviderName, String businessObjectDefinitionDescription, String displayName, List<Attribute> attributes)
    {
        BusinessObjectDefinitionCreateRequest request = new BusinessObjectDefinitionCreateRequest();
        request.setNamespace(namespaceCode);
        request.setBusinessObjectDefinitionName(businessObjectDefinitionName);
        request.setDataProviderName(dataProviderName);
        request.setDescription(businessObjectDefinitionDescription);
        request.setDisplayName(displayName);
        request.setAttributes(attributes);
        return request;
    }

    /**
     * Creates a business object data definition metadata update request.
     *
     * @param businessObjectDefinitionDescription the description of the business object definition
     *
     * @return the newly created business object definition update request
     */
    public BusinessObjectDefinitionDescriptiveInfoUpdateRequest createBusinessObjectDefinitionDescriptiveInfoUpdateRequest(
        String businessObjectDefinitionDescription, String displayName)
    {
        BusinessObjectDefinitionDescriptiveInfoUpdateRequest request = new BusinessObjectDefinitionDescriptiveInfoUpdateRequest();
        request.setDescription(businessObjectDefinitionDescription);
        request.setDisplayName(displayName);
        return request;
    }

    /**
     * Creates a business object data definition update request.
     *
     * @param businessObjectDefinitionDescription the description of the business object definition
     *
     * @return the newly created business object definition update request
     */
    public BusinessObjectDefinitionUpdateRequest createBusinessObjectDefinitionUpdateRequest(String businessObjectDefinitionDescription, String displayName,
        List<Attribute> attributes)
    {
        BusinessObjectDefinitionUpdateRequest request = new BusinessObjectDefinitionUpdateRequest();
        request.setDescription(businessObjectDefinitionDescription);
        request.setDisplayName(displayName);
        request.setAttributes(attributes);
        return request;
    }

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
     * Returns an expected string representation of the specified business object definition key.
     *
     * @param businessObjectDefinitionKey the key of the business object definition
     *
     * @return the string representation of the specified business object definition key
     */
    public String getExpectedBusinessObjectDefinitionKeyAsString(BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        return getExpectedBusinessObjectDefinitionKeyAsString(businessObjectDefinitionKey.getNamespace(),
            businessObjectDefinitionKey.getBusinessObjectDefinitionName());
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
