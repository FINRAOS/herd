package org.finra.herd.service;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchResult;

/**
 * Test Business Object Data Search service
 * 
 * @author k22201
 *
 */
public class BusinessObjectDataSearchServiceTest extends AbstractServiceTest
{

    @Test
    public void testSearchBusinessObjectData()
    {
        createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<BusinessObjectDataSearchFilter>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<BusinessObjectDataSearchKey>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);

        BusinessObjectDataSearchResult result = this.businessObjectDataService.searchBusinessObjectData(request);

        Assert.isTrue(result.getBusinessObjectDataElements().size() == 2);

    }

}
