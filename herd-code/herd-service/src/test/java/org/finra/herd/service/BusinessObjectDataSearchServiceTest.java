package org.finra.herd.service;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.util.Assert;

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
        createDatabaseEntitiesForBusinessObjectDataSearchTesting();

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
        
        for(BusinessObjectData data : result.getBusinessObjectDataElements())
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
        }

    }
    @Test
    public void testSearchBusinessObjectDataWithPartitionFilterValues()
    {
        createDatabaseEntitiesForBusinessObjectDataSearchTesting();

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
        createDatabaseEntitiesForBusinessObjectDataSearchTesting();

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
}
