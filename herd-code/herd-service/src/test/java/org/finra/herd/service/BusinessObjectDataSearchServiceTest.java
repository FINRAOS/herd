package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.util.Assert;

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

        BusinessObjectDataSearchResult result = this.businessObjectDataService.searchBusinessObjectData(request);

        Assert.isTrue(result.getBusinessObjectDataElements().size() == 2);

        for (BusinessObjectData data : result.getBusinessObjectDataElements())
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
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

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<PartitionValueFilter>();
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setPartitionKey(PARTITION_KEY);
        List<String> values = new ArrayList<String>();
        values.add(PARTITION_VALUE);
        partitionValueFilter.setPartitionValues(values);
        key.setPartitionValueFilters(partitionValueFilters);

        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);

        BusinessObjectDataSearchResult result = this.businessObjectDataService.searchBusinessObjectData(request);
        //this should be zero, as no schema column is registered
        Assert.isTrue(result.getBusinessObjectDataElements().size() == 0);
    }

    @Test
    public void testSearchBusinessObjectDataWithPartitionFilterBadRequest()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<PartitionValueFilter>();
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setLatestAfterPartitionValue(new LatestAfterPartitionValue("A"));
        partitionValueFilter.setLatestBeforePartitionValue(new LatestBeforePartitionValue("B"));
        key.setPartitionValueFilters(partitionValueFilters);

        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);

        try
        {
            businessObjectDataService.searchBusinessObjectData(request);
            fail("Should not get here, as IllegalArgumentException should be thrown");
        }
        catch (IllegalArgumentException ex)
        {
        }

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
            businessObjectDataService.searchBusinessObjectData(request);
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
            businessObjectDataService.searchBusinessObjectData(request);
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
            businessObjectDataService.searchBusinessObjectData(request);
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

        BusinessObjectDataSearchResult result = this.businessObjectDataService.searchBusinessObjectData(request);      
        List<BusinessObjectData> resultList = result.getBusinessObjectDataElements();
        assertEquals(1, resultList.size());

        for (BusinessObjectData data : resultList)
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
            Assert.isTrue(ATTRIBUTE_NAME_1_MIXED_CASE.equals(data.getAttributes().get(0).getName()));
            Assert.isTrue(ATTRIBUTE_VALUE_1.equals(data.getAttributes().get(0).getValue()));
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

        BusinessObjectDataSearchResult result = this.businessObjectDataService.searchBusinessObjectData(request);      
        List<BusinessObjectData> resultList = result.getBusinessObjectDataElements();
        assertEquals(1, resultList.size());

        for (BusinessObjectData data : resultList)
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
            Assert.isTrue(ATTRIBUTE_NAME_1_MIXED_CASE.equals(data.getAttributes().get(0).getName()));
            Assert.isTrue(ATTRIBUTE_VALUE_1.equals(data.getAttributes().get(0).getValue()));
        }
    }
    
}
