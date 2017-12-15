package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.api.xml.AttributeValueFilter;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchResult;
import org.finra.herd.model.api.xml.LatestAfterPartitionValue;
import org.finra.herd.model.api.xml.LatestBeforePartitionValue;
import org.finra.herd.model.api.xml.PartitionValueFilter;

/**
 * Test Business Object Data Search service
 */
public class BusinessObjectDataSearchServiceTest extends AbstractServiceTest
{
    /**
     * The default page number for the business object data search
     */
    private static final Integer DEFAULT_PAGE_NUMBER = 1;

    /**
     * The default page size for the business object data search
     */
    private static final Integer DEFAULT_PAGE_SIZE = 1_000;
    
    @Test
    public void testSearchBusinessObjectData()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);

        BusinessObjectDataSearchResult result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, request);

        assertTrue(result.getBusinessObjectDataElements().size() == 2);

        for (BusinessObjectData data : result.getBusinessObjectDataElements())
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
        }

    }

    @Test
    public void testSearchBusinessObjectDataWithPartitionFilterValues()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setPartitionKey(PARTITION_KEY);
        List<String> values = new ArrayList<>();
        values.add(PARTITION_VALUE);
        partitionValueFilter.setPartitionValues(values);
        key.setPartitionValueFilters(partitionValueFilters);

        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);

        BusinessObjectDataSearchResult result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, request);
        //this should be zero, as no schema column is registered
        assertTrue(result.getBusinessObjectDataElements().size() == 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSearchBusinessObjectDataWithPartitionFilterBadRequest()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setLatestAfterPartitionValue(new LatestAfterPartitionValue("A"));
        partitionValueFilter.setLatestBeforePartitionValue(new LatestBeforePartitionValue("B"));
        key.setPartitionValueFilters(partitionValueFilters);

        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);

        businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, request);
        fail("Should not get here, as IllegalArgumentException should be thrown");
    }
    
    @Test
    public void testSearchBusinessObjectDataWithAttributeFilterBadRequest()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);
        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        //null attribute name and value
        attributeValueFilters.add(new AttributeValueFilter(null, null));
        key.setAttributeValueFilters(attributeValueFilters);

        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);

        try
        {
            businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, request);
            fail("Should not get here, as IllegalArgumentException should be thrown");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals("Either attribute name or value filter must exist.", ex.getMessage());
        }
        
        //empty attribute name and null attribute value
        attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(" ", null));
        key.setAttributeValueFilters(attributeValueFilters);
        try
        {
            businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, request);
            fail("Should not get here, as IllegalArgumentException should be thrown");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals("Either attribute name or value filter must exist.", ex.getMessage());
        }
        
       //empty attribute name and empty attribute value
        attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(" ", ""));
        key.setAttributeValueFilters(attributeValueFilters);
        try
        {
            businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, request);
            fail("Should not get here, as IllegalArgumentException should be thrown");
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals("Either attribute name or value filter must exist.", ex.getMessage());
        }
    }
    
    @Test
    public void testSearchBusinessObjectDataWithAttributeFilterValues()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

       businessObjectDataAttributeDaoTestHelper
                .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);
        
        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);

        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));

        key.setAttributeValueFilters(attributeValueFilters);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);

        BusinessObjectDataSearchResult result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, request);      
        List<BusinessObjectData> resultList = result.getBusinessObjectDataElements();
        assertEquals(1, resultList.size());

        for (BusinessObjectData data : resultList)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(ATTRIBUTE_NAME_1_MIXED_CASE, data.getAttributes().get(0).getName());
            assertEquals(ATTRIBUTE_VALUE_1, data.getAttributes().get(0).getValue());
        }
    }
    
    @Test
    public void testSearchBusinessObjectDataWithAttributeFilterValuesWithMixedCaseAndSpace()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        businessObjectDataAttributeDaoTestHelper
                .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);
        
        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);

        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter("  " + ATTRIBUTE_NAME_1_MIXED_CASE.toLowerCase() + "  ", ATTRIBUTE_VALUE_1));

        key.setAttributeValueFilters(attributeValueFilters);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);

        BusinessObjectDataSearchResult result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, request);      
        List<BusinessObjectData> resultList = result.getBusinessObjectDataElements();
        assertEquals(1, resultList.size());

        for (BusinessObjectData data : resultList)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(ATTRIBUTE_NAME_1_MIXED_CASE, data.getAttributes().get(0).getName());
            assertEquals(ATTRIBUTE_VALUE_1, data.getAttributes().get(0).getValue());
        }
    }
    
    @Test
    public void testSearchBusinessObjectDataWithAttributeFilterValuesWithMultipleFilters()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        businessObjectDataAttributeDaoTestHelper
                .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                    null, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);
        
        businessObjectDataAttributeDaoTestHelper
        .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            null, DATA_VERSION, ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2);
        
        businessObjectDataAttributeDaoTestHelper
        .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            null, DATA_VERSION, ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3);
        
        businessObjectDataAttributeDaoTestHelper
        .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
            null, DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);
        
        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);

        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1));
        attributeValueFilters.add(new AttributeValueFilter(ATTRIBUTE_NAME_2_MIXED_CASE, null));
        
        key.setAttributeValueFilters(attributeValueFilters);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);

        BusinessObjectDataSearchResult result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, request);      
        List<BusinessObjectData> resultList = result.getBusinessObjectDataElements();
        assertEquals(1, resultList.size());

        for (BusinessObjectData data : resultList)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());

            assertEquals(2, data.getAttributes().size());
            boolean foundCase1 = false, foundCase2 = false;
            for (int i = 0; i < data.getAttributes().size(); i++)
            {
               if (ATTRIBUTE_NAME_1_MIXED_CASE.equals(data.getAttributes().get(i).getName()))
               {
                   assertEquals(ATTRIBUTE_VALUE_1, data.getAttributes().get(i).getValue());
                   foundCase1 = true;
               }
               if (ATTRIBUTE_NAME_2_MIXED_CASE.equals(data.getAttributes().get(i).getName()))
               {
                   assertEquals(ATTRIBUTE_VALUE_2, data.getAttributes().get(i).getValue());
                   foundCase2 = true;
               }              
            }
            assertTrue(foundCase1 && foundCase2);
          }
    }

    @Test
    public void testSearchBusinessObjectDataWithPageNumPageSize()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        BusinessObjectDataSearchRequest request = businessObjectDataServiceTestHelper.createSimpleBusinessObjectDataSearchRequest(NAMESPACE, BDEF_NAME);

        // Test getting the first page
        BusinessObjectDataSearchResult result = businessObjectDataService.searchBusinessObjectData(1, 1, request);

        assertTrue(result.getBusinessObjectDataElements().size() == 1);

        for (BusinessObjectData data : result.getBusinessObjectDataElements())
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertEquals(FORMAT_USAGE_CODE, data.getBusinessObjectFormatUsage());
        }

        // Test getting the second page
        result = businessObjectDataService.searchBusinessObjectData(2, 1, request);

        assertTrue(result.getBusinessObjectDataElements().size() == 1);

        for (BusinessObjectData data : result.getBusinessObjectDataElements())
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertEquals(FORMAT_USAGE_CODE_2, data.getBusinessObjectFormatUsage());
        }

        // Test getting a larger page than there are results remaining
        result = businessObjectDataService.searchBusinessObjectData(1, 3, request);

        assertTrue(result.getBusinessObjectDataElements().size() == 2);

        // Test getting a page that does not exist
        result = businessObjectDataService.searchBusinessObjectData(3, 1, request);

        assertTrue(result.getBusinessObjectDataElements().size() == 0);
    }
}
