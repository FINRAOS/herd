package org.finra.herd.rest;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchResult;

/**
 * This class tests searchBusinessObjectData functionality within the business object data REST controller.
 */
public class BusinessObjectDataRestControllerSearchBusinessObjectDataTest extends AbstractRestTest
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

        BusinessObjectDataSearchResult result = businessObjectDataRestController.searchBusinessObjectData(request);

        Assert.isTrue(result.getBusinessObjectDataElements().size() == 2);

        for (BusinessObjectData data : result.getBusinessObjectDataElements())
        {
            Assert.isTrue(NAMESPACE.equals(data.getNamespace()));
            Assert.isTrue(BDEF_NAME.equals(data.getBusinessObjectDefinitionName()));
        }

    }
}
