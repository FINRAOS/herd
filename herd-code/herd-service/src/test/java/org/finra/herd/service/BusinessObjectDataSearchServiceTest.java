package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;

import org.finra.herd.model.api.xml.AttributeValueFilter;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.api.xml.PartitionValueRange;
import org.finra.herd.model.api.xml.RegistrationDateRangeFilter;
import org.finra.herd.model.dto.BusinessObjectDataSearchResultPagingInfoDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;

public class BusinessObjectDataSearchServiceTest extends AbstractServiceTest
{
    /**
     * The default page number for the business object data search.
     */
    private static final Integer DEFAULT_PAGE_NUMBER = 1;

    /**
     * The default page size for the business object data search.
     */
    private static final Integer DEFAULT_PAGE_SIZE = (Integer) ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_MAX_PAGE_SIZE.getDefaultValue();

    /**
     * The page size for the business object data search.
     */
    private static final Integer PAGE_SIZE = 100;

    @Test
    public void testSearchBusinessObjectDataAttributeValueFilters()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_2_MIXED_CASE, ATTRIBUTE_VALUE_2);

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_3_MIXED_CASE, ATTRIBUTE_VALUE_3);

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

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

        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, request);
        List<BusinessObjectData> resultList = result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements();
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

        // Validate the paging information.
        assertEquals(Long.valueOf(DEFAULT_PAGE_NUMBER), result.getPageNum());
        assertEquals(Long.valueOf(PAGE_SIZE), result.getPageSize());
        assertEquals(Long.valueOf(1), result.getPageCount());
        assertEquals(Long.valueOf(1), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(1), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
    }

    @Test
    public void testSearchBusinessObjectDataAttributeValueFiltersMissingRequiredParameters()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKeys.add(businessObjectDataSearchKey);
        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);

        // Try to search with a null attribute name and a null attribute value.
        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(null, null));
        businessObjectDataSearchKey.setAttributeValueFilters(attributeValueFilters);
        try
        {
            businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, request);
            fail();
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals("Either attribute name or attribute value filter must be specified.", ex.getMessage());
        }

        // Try to search with an empty attribute name and a null attribute value.
        attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(" ", null));
        businessObjectDataSearchKey.setAttributeValueFilters(attributeValueFilters);
        try
        {
            businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, request);
            fail();
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals("Either attribute name or attribute value filter must be specified.", ex.getMessage());
        }

        // Try to search with an empty attribute name and empty attribute value.
        attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(" ", ""));
        businessObjectDataSearchKey.setAttributeValueFilters(attributeValueFilters);
        try
        {
            businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, request);
            fail();
        }
        catch (IllegalArgumentException ex)
        {
            assertEquals("Either attribute name or attribute value filter must be specified.", ex.getMessage());
        }
    }

    @Test
    public void testSearchBusinessObjectDataAttributeValueFiltersSingleFilter()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

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

        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, request);
        List<BusinessObjectData> resultList = result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements();
        assertEquals(1, resultList.size());

        for (BusinessObjectData data : resultList)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(ATTRIBUTE_NAME_1_MIXED_CASE, data.getAttributes().get(0).getName());
            assertEquals(ATTRIBUTE_VALUE_1, data.getAttributes().get(0).getValue());
        }

        // Validate the paging information.
        assertEquals(Long.valueOf(DEFAULT_PAGE_NUMBER), result.getPageNum());
        assertEquals(Long.valueOf(PAGE_SIZE), result.getPageSize());
        assertEquals(Long.valueOf(1), result.getPageCount());
        assertEquals(Long.valueOf(1), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(1), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
    }

    @Test
    public void testSearchBusinessObjectDataAttributeValueFiltersTrimAttributeName()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        businessObjectDataAttributeDaoTestHelper
            .createBusinessObjectDataAttributeEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null,
                DATA_VERSION, ATTRIBUTE_NAME_1_MIXED_CASE, ATTRIBUTE_VALUE_1);

        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);

        List<AttributeValueFilter> attributeValueFilters = new ArrayList<>();
        attributeValueFilters.add(new AttributeValueFilter(addWhitespace(ATTRIBUTE_NAME_1_MIXED_CASE), ATTRIBUTE_VALUE_1));

        key.setAttributeValueFilters(attributeValueFilters);
        businessObjectDataSearchKeys.add(key);

        BusinessObjectDataSearchFilter filter = new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys);
        filters.add(filter);
        request.setBusinessObjectDataSearchFilters(filters);

        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, request);
        List<BusinessObjectData> resultList = result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements();
        assertEquals(1, resultList.size());

        for (BusinessObjectData data : resultList)
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(ATTRIBUTE_NAME_1_MIXED_CASE, data.getAttributes().get(0).getName());
            assertEquals(ATTRIBUTE_VALUE_1, data.getAttributes().get(0).getValue());
        }

        // Validate the paging information.
        assertEquals(Long.valueOf(DEFAULT_PAGE_NUMBER), result.getPageNum());
        assertEquals(Long.valueOf(PAGE_SIZE), result.getPageSize());
        assertEquals(Long.valueOf(1), result.getPageCount());
        assertEquals(Long.valueOf(1), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(1), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
    }

    @Test
    public void testSearchBusinessObjectDataLatestValidFilterPagingTraverseAllPages()
    {
        // Create test data.
        List<BusinessObjectDataEntity> expectedBusinessObjectDataEntities =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchWithLatestValidFilterTesting();
        assertEquals(3, CollectionUtils.size(expectedBusinessObjectDataEntities));

        // Create a simple search request with the latest valid filter.
        BusinessObjectDataSearchRequest request = businessObjectDataServiceTestHelper.createSimpleBusinessObjectDataSearchRequest(NAMESPACE, BDEF_NAME);
        request.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setFilterOnLatestValidVersion(FILTER_ON_LATEST_VALID_VERSION);

        // Go through all expected entities - one page with a single search result at a time.
        int pageNum = 0;
        for (BusinessObjectDataEntity expectedBusinessObjectDataEntity : expectedBusinessObjectDataEntities)
        {
            // Get the relative page with page size set to a single response.
            BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(++pageNum, 1, request);

            // Validate the search results.
            assertNotNull(result);
            assertNotNull(result.getBusinessObjectDataSearchResult());
            assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));
            assertEquals(new BusinessObjectData(expectedBusinessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME,
                    expectedBusinessObjectDataEntity.getBusinessObjectFormat().getUsage(),
                    expectedBusinessObjectDataEntity.getBusinessObjectFormat().getFileType().getCode(),
                    expectedBusinessObjectDataEntity.getBusinessObjectFormat().getBusinessObjectFormatVersion(),
                    expectedBusinessObjectDataEntity.getBusinessObjectFormat().getPartitionKey(), expectedBusinessObjectDataEntity.getPartitionValue(),
                    NULL_AS_SUBPARTITION_VALUES, expectedBusinessObjectDataEntity.getVersion(), expectedBusinessObjectDataEntity.getLatestVersion(),
                    expectedBusinessObjectDataEntity.getStatus().getCode(), NULL_AS_STORAGE_UNITS, NULL_AS_ATTRIBUTES, NULL_AS_BUSINESS_OBJECT_DATA_PARENTS,
                    NULL_AS_BUSINESS_OBJECT_DATA_CHILDREN, NULL_AS_BUSINESS_OBJECT_DATA_STATUS_HISTORY, NO_RETENTION_EXPIRATION_DATE, NO_CREATED_BY,
                    NO_CREATED_ON), result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().get(0));

            // Validate the paging information.
            assertEquals(Long.valueOf(pageNum), result.getPageNum());
            assertEquals(Long.valueOf(1), result.getPageSize());
            assertEquals(Long.valueOf(CollectionUtils.size(expectedBusinessObjectDataEntities)), result.getPageCount());
            assertEquals(Long.valueOf(1), result.getTotalRecordsOnPage());
            assertEquals(Long.valueOf(CollectionUtils.size(expectedBusinessObjectDataEntities)), result.getTotalRecordCount());
            assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
        }

        // Get the first page with page size set to 2.
        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(1, 2, request);

        // Validate the search results.
        assertNotNull(result);
        assertNotNull(result.getBusinessObjectDataSearchResult());
        assertEquals(2, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));
        assertEquals(Long.valueOf(expectedBusinessObjectDataEntities.get(0).getId()),
            Long.valueOf(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().get(0).getId()));
        assertEquals(Long.valueOf(expectedBusinessObjectDataEntities.get(1).getId()),
            Long.valueOf(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().get(1).getId()));

        // Validate the paging information.
        assertEquals(Long.valueOf(1), result.getPageNum());
        assertEquals(Long.valueOf(2), result.getPageSize());
        assertEquals(Long.valueOf(2), result.getPageCount());
        assertEquals(Long.valueOf(2), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(3), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());

        // Get the second page with page size set to 2.
        result = businessObjectDataService.searchBusinessObjectData(2, 2, request);

        // Validate the search results.
        assertNotNull(result);
        assertNotNull(result.getBusinessObjectDataSearchResult());
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));
        assertEquals(Long.valueOf(expectedBusinessObjectDataEntities.get(2).getId()),
            Long.valueOf(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().get(0).getId()));

        // Validate the paging information.
        assertEquals(Long.valueOf(2), result.getPageNum());
        assertEquals(Long.valueOf(2), result.getPageSize());
        assertEquals(Long.valueOf(2), result.getPageCount());
        assertEquals(Long.valueOf(1), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(3), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());

        // Test getting a larger page than there are expected search results.
        result = businessObjectDataService.searchBusinessObjectData(1, CollectionUtils.size(expectedBusinessObjectDataEntities) + 2, request);
        assertEquals(CollectionUtils.size(expectedBusinessObjectDataEntities),
            result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        // Validate the paging information.
        assertEquals(Long.valueOf(1), result.getPageNum());
        assertEquals(Long.valueOf(CollectionUtils.size(expectedBusinessObjectDataEntities) + 2), result.getPageSize());
        assertEquals(Long.valueOf(1), result.getPageCount());
        assertEquals(Long.valueOf(CollectionUtils.size(expectedBusinessObjectDataEntities)), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(CollectionUtils.size(expectedBusinessObjectDataEntities)), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());

        // Test getting a page that does not exist.
        // This is a case when number of records to be skipped due to specified page number and page size is equal to the total record count.
        result = businessObjectDataService.searchBusinessObjectData(4, 1, request);
        assertEquals(0, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        // Validate the paging information.
        assertEquals(Long.valueOf(4), result.getPageNum());
        assertEquals(Long.valueOf(1), result.getPageSize());
        assertEquals(Long.valueOf(CollectionUtils.size(expectedBusinessObjectDataEntities)), result.getPageCount());
        assertEquals(Long.valueOf(0), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(CollectionUtils.size(expectedBusinessObjectDataEntities)), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());

        // Test getting a page that does not exist.
        // This is a case when number of records to be skipped due to specified page number and page size is greater than the total record count.
        result = businessObjectDataService.searchBusinessObjectData(5, 1, request);
        assertEquals(0, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        // Validate the paging information.
        assertEquals(Long.valueOf(5), result.getPageNum());
        assertEquals(Long.valueOf(1), result.getPageSize());
        assertEquals(Long.valueOf(CollectionUtils.size(expectedBusinessObjectDataEntities)), result.getPageCount());
        assertEquals(Long.valueOf(0), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(CollectionUtils.size(expectedBusinessObjectDataEntities)), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
    }

    @Test
    public void testSearchBusinessObjectDataLatestValidFilterRawSearchResultsGreaterThanSearchQueryPaginationSize() throws Exception
    {
        // Create test data.
        List<BusinessObjectDataEntity> expectedBusinessObjectDataEntities =
            businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchWithLatestValidFilterTesting();
        assertEquals(3, CollectionUtils.size(expectedBusinessObjectDataEntities));

        // Create a simple search request with the latest valid filter.
        BusinessObjectDataSearchRequest request = businessObjectDataServiceTestHelper.createSimpleBusinessObjectDataSearchRequest(NAMESPACE, BDEF_NAME);
        request.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setFilterOnLatestValidVersion(FILTER_ON_LATEST_VALID_VERSION);

        // Override configuration for business object data search query pagination size to be small enough to require multiple calls to the database to get all
        // raw business object data search results.
        int maxBusinessObjectDataSearchQueryPaginationSize = 2;
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_QUERY_PAGINATION_SIZE.getKey(), maxBusinessObjectDataSearchQueryPaginationSize);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Get the first page with page size set to 2.
            BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(1, 2, request);

            // Validate the search results.
            assertNotNull(result);
            assertNotNull(result.getBusinessObjectDataSearchResult());
            assertEquals(2, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));
            assertEquals(Long.valueOf(expectedBusinessObjectDataEntities.get(0).getId()),
                Long.valueOf(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().get(0).getId()));
            assertEquals(Long.valueOf(expectedBusinessObjectDataEntities.get(1).getId()),
                Long.valueOf(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().get(1).getId()));

            // Validate the paging information.
            assertEquals(Long.valueOf(1), result.getPageNum());
            assertEquals(Long.valueOf(2), result.getPageSize());
            assertEquals(Long.valueOf(2), result.getPageCount());
            assertEquals(Long.valueOf(2), result.getTotalRecordsOnPage());
            assertEquals(Long.valueOf(3), result.getTotalRecordCount());
            assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());

            // Get the second page with page size set to 2.
            result = businessObjectDataService.searchBusinessObjectData(2, 2, request);

            // Validate the search results.
            assertNotNull(result);
            assertNotNull(result.getBusinessObjectDataSearchResult());
            assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));
            assertEquals(Long.valueOf(expectedBusinessObjectDataEntities.get(2).getId()),
                Long.valueOf(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().get(0).getId()));

            // Validate the paging information.
            assertEquals(Long.valueOf(2), result.getPageNum());
            assertEquals(Long.valueOf(2), result.getPageSize());
            assertEquals(Long.valueOf(2), result.getPageCount());
            assertEquals(Long.valueOf(1), result.getTotalRecordsOnPage());
            assertEquals(Long.valueOf(3), result.getTotalRecordCount());
            assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testSearchBusinessObjectDataNoFilters()
    {
        // Create business object data entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        // Search business object data by specifying all business object data search key parameters, except for filters.
        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE,
            new BusinessObjectDataSearchRequest(Collections.singletonList(new BusinessObjectDataSearchFilter(Collections.singletonList(
                new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                    NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION))))));

        // Validate the results.
        assertEquals(1, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());
        for (BusinessObjectData businessObjectData : result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements())
        {
            assertEquals(NAMESPACE, businessObjectData.getNamespace());
            assertEquals(BDEF_NAME, businessObjectData.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_USAGE_CODE, businessObjectData.getBusinessObjectFormatUsage());
            assertEquals(FORMAT_FILE_TYPE_CODE, businessObjectData.getBusinessObjectFormatFileType());
            assertEquals(FORMAT_VERSION, Integer.valueOf(businessObjectData.getBusinessObjectFormatVersion()));
        }

        // Validate the paging information.
        assertEquals(Long.valueOf(DEFAULT_PAGE_NUMBER), result.getPageNum());
        assertEquals(Long.valueOf(PAGE_SIZE), result.getPageSize());
        assertEquals(Long.valueOf(1), result.getPageCount());
        assertEquals(Long.valueOf(1), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(1), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
    }

    @Test
    public void testSearchBusinessObjectDataNoFiltersLowerCaseParameters()
    {
        // Create business object data entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        // Search business object data by specifying all business object data search key parameters in lowercase.
        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE,
            new BusinessObjectDataSearchRequest(Collections.singletonList(new BusinessObjectDataSearchFilter(Collections.singletonList(
                new BusinessObjectDataSearchKey(NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), FORMAT_USAGE_CODE.toLowerCase(),
                    FORMAT_FILE_TYPE_CODE.toLowerCase(), FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS, NO_REGISTRATION_DATE_RANGE_FILTER,
                    NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION))))));

        // Validate the results.
        assertEquals(1, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());
        for (BusinessObjectData businessObjectData : result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements())
        {
            assertEquals(NAMESPACE, businessObjectData.getNamespace());
            assertEquals(BDEF_NAME, businessObjectData.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_USAGE_CODE, businessObjectData.getBusinessObjectFormatUsage());
            assertEquals(FORMAT_FILE_TYPE_CODE, businessObjectData.getBusinessObjectFormatFileType());
            assertEquals(FORMAT_VERSION, Integer.valueOf(businessObjectData.getBusinessObjectFormatVersion()));
        }

        // Validate the paging information.
        assertEquals(Long.valueOf(DEFAULT_PAGE_NUMBER), result.getPageNum());
        assertEquals(Long.valueOf(PAGE_SIZE), result.getPageSize());
        assertEquals(Long.valueOf(1), result.getPageCount());
        assertEquals(Long.valueOf(1), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(1), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
    }

    @Test
    public void testSearchBusinessObjectDataNoFiltersMissingOptionalParametersPassedAsNulls()
    {
        // Create business object data entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        // Search business object data by specifying only parameters that are required for a business object data search key.
        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE,
            new BusinessObjectDataSearchRequest(Collections.singletonList(new BusinessObjectDataSearchFilter(Collections.singletonList(
                new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION,
                    NO_PARTITION_VALUE_FILTERS, NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION,
                    NO_FILTER_ON_RETENTION_EXPIRATION))))));

        // Validate the results.
        assertEquals(2, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());
        for (BusinessObjectData businessObjectData : result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements())
        {
            assertEquals(NAMESPACE, businessObjectData.getNamespace());
            assertEquals(BDEF_NAME, businessObjectData.getBusinessObjectDefinitionName());
        }

        // Validate the paging information.
        assertEquals(Long.valueOf(DEFAULT_PAGE_NUMBER), result.getPageNum());
        assertEquals(Long.valueOf(PAGE_SIZE), result.getPageSize());
        assertEquals(Long.valueOf(1), result.getPageCount());
        assertEquals(Long.valueOf(2), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(2), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
    }

    @Test
    public void testSearchBusinessObjectDataNoFiltersMissingOptionalParametersPassedAsWhitespace()
    {
        // Create business object data entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        // Search business object data by specifying only parameters that are required for a business object data search key.
        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE,
            new BusinessObjectDataSearchRequest(Collections.singletonList(new BusinessObjectDataSearchFilter(Collections.singletonList(
                new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, BLANK_TEXT, BLANK_TEXT, NO_FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                    NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION))))));

        // Validate the results.
        assertEquals(2, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());
        for (BusinessObjectData businessObjectData : result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements())
        {
            assertEquals(NAMESPACE, businessObjectData.getNamespace());
            assertEquals(BDEF_NAME, businessObjectData.getBusinessObjectDefinitionName());
        }

        // Validate the paging information.
        assertEquals(Long.valueOf(DEFAULT_PAGE_NUMBER), result.getPageNum());
        assertEquals(Long.valueOf(PAGE_SIZE), result.getPageSize());
        assertEquals(Long.valueOf(1), result.getPageCount());
        assertEquals(Long.valueOf(2), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(2), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
    }

    @Test
    public void testSearchBusinessObjectDataNoFiltersMissingRequiredParameters()
    {
        // Try to search business object data without specifying a namespace.
        try
        {
            businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, new BusinessObjectDataSearchRequest(Collections.singletonList(
                new BusinessObjectDataSearchFilter(Collections.singletonList(
                    new BusinessObjectDataSearchKey(BLANK_TEXT, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                        NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION,
                        NO_FILTER_ON_RETENTION_EXPIRATION))))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to search business object data without specifying a business object definition name.
        try
        {
            businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, new BusinessObjectDataSearchRequest(Collections.singletonList(
                new BusinessObjectDataSearchFilter(Collections.singletonList(
                    new BusinessObjectDataSearchKey(BDEF_NAMESPACE, BLANK_TEXT, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                        NO_PARTITION_VALUE_FILTERS, NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION,
                        NO_FILTER_ON_RETENTION_EXPIRATION))))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testSearchBusinessObjectDataNoFiltersPagingMaxRecordsExceeded() throws Exception
    {
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION,
                true, "VALID");
        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE, null, DATA_VERSION,
                true, "INVALID");

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, PARTITION_VALUE, null,
                DATA_VERSION, true, "INVALID");

        businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME_2, FORMAT_USAGE_CODE_2, FORMAT_FILE_TYPE_CODE_2, FORMAT_VERSION_2, PARTITION_VALUE, null,
                DATA_VERSION, true, "VALID");

        // Override configuration.
        int maxBusinessObjectDataSearchResultCount = 2;
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_MAX_RESULT_COUNT.getKey(), maxBusinessObjectDataSearchResultCount);
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, new BusinessObjectDataSearchRequest(Collections
                .singletonList(new BusinessObjectDataSearchFilter(Collections.singletonList(
                    new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, NO_FORMAT_USAGE_CODE, NO_FORMAT_FILE_TYPE_CODE, NO_FORMAT_VERSION,
                        NO_PARTITION_VALUE_FILTERS, NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION,
                        NO_FILTER_ON_RETENTION_EXPIRATION))))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Result limit of %d exceeded. Modify filters to further limit results.", maxBusinessObjectDataSearchResultCount),
                e.getMessage());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }

    @Test
    public void testSearchBusinessObjectDataNoFiltersPagingPageSizeGreaterThanMaximumPageSize()
    {
        // Get the maximum page size configured in the system.
        int maxPageSize = configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_MAX_PAGE_SIZE, Integer.class);

        // Try to search business object data.
        try
        {
            businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, maxPageSize + 1,
                businessObjectDataServiceTestHelper.createSimpleBusinessObjectDataSearchRequest(NAMESPACE, BDEF_NAME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("A pageSize less than %d must be specified.", maxPageSize), e.getMessage());
        }
    }

    @Test
    public void testSearchBusinessObjectDataNoFiltersPagingTraverseAllPages()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        BusinessObjectDataSearchRequest request = businessObjectDataServiceTestHelper.createSimpleBusinessObjectDataSearchRequest(NAMESPACE, BDEF_NAME);

        // Test getting the first page.
        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(1, 1, request);

        assertEquals(1, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        for (BusinessObjectData data : result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements())
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertEquals(FORMAT_USAGE_CODE, data.getBusinessObjectFormatUsage());
        }

        // Validate the paging information.
        assertEquals(Long.valueOf(1), result.getPageNum());
        assertEquals(Long.valueOf(1), result.getPageSize());
        assertEquals(Long.valueOf(2), result.getPageCount());
        assertEquals(Long.valueOf(1), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(2), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());

        // Test getting the second page.
        result = businessObjectDataService.searchBusinessObjectData(2, 1, request);

        assertEquals(1, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        for (BusinessObjectData data : result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements())
        {
            assertEquals(NAMESPACE, data.getNamespace());
            assertEquals(BDEF_NAME, data.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_FILE_TYPE_CODE, data.getBusinessObjectFormatFileType());
            assertEquals(FORMAT_USAGE_CODE_2, data.getBusinessObjectFormatUsage());
        }

        // Validate the paging information.
        assertEquals(Long.valueOf(2), result.getPageNum());
        assertEquals(Long.valueOf(1), result.getPageSize());
        assertEquals(Long.valueOf(2), result.getPageCount());
        assertEquals(Long.valueOf(1), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(2), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());

        // Test getting a larger page than there are results remaining
        result = businessObjectDataService.searchBusinessObjectData(1, 3, request);

        assertEquals(2, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        // Validate the paging information.
        assertEquals(Long.valueOf(1), result.getPageNum());
        assertEquals(Long.valueOf(3), result.getPageSize());
        assertEquals(Long.valueOf(1), result.getPageCount());
        assertEquals(Long.valueOf(2), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(2), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());

        // Test getting a page that does not exist.
        result = businessObjectDataService.searchBusinessObjectData(3, 1, request);

        assertEquals(0, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        // Validate the paging information.
        assertEquals(Long.valueOf(3), result.getPageNum());
        assertEquals(Long.valueOf(1), result.getPageSize());
        assertEquals(Long.valueOf(2), result.getPageCount());
        assertEquals(Long.valueOf(0), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(2), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
    }

    @Test
    public void testSearchBusinessObjectDataNoFiltersRelativeEntitiesNoExist()
    {
        // Create business object data entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        // Search business object data by specifying all business object data search key parameters, except for filters.
        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE,
            new BusinessObjectDataSearchRequest(Collections.singletonList(new BusinessObjectDataSearchFilter(Collections.singletonList(
                new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                    NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION))))));

        // Validate the results.
        assertEquals(1, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        // Search business object data by specifying a non-existing namespace.
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, new BusinessObjectDataSearchRequest(Collections
            .singletonList(new BusinessObjectDataSearchFilter(Collections.singletonList(
                new BusinessObjectDataSearchKey(I_DO_NOT_EXIST, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                    NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION))))));

        // Validate the results.
        assertEquals(0, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        // Search business object data by specifying a non-existing business object definition name.
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, new BusinessObjectDataSearchRequest(Collections
            .singletonList(new BusinessObjectDataSearchFilter(Collections.singletonList(
                new BusinessObjectDataSearchKey(NAMESPACE, I_DO_NOT_EXIST, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                    NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION))))));

        // Validate the results.
        assertEquals(0, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        // Search business object data by specifying a non-existing business object format usage.
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, new BusinessObjectDataSearchRequest(Collections
            .singletonList(new BusinessObjectDataSearchFilter(Collections.singletonList(
                new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, I_DO_NOT_EXIST, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                    NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION))))));

        // Validate the results.
        assertEquals(0, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        // Search business object data by specifying a non-existing business object format file type.
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, new BusinessObjectDataSearchRequest(Collections
            .singletonList(new BusinessObjectDataSearchFilter(Collections.singletonList(
                new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, I_DO_NOT_EXIST, FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS,
                    NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION))))));

        // Validate the results.
        assertEquals(0, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        // Search business object data by specifying a non-existing business object format version.
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, new BusinessObjectDataSearchRequest(Collections
            .singletonList(new BusinessObjectDataSearchFilter(Collections.singletonList(
                new BusinessObjectDataSearchKey(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION_2, NO_PARTITION_VALUE_FILTERS,
                    NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION))))));

        // Validate the results.
        assertEquals(0, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());
    }

    @Test
    public void testSearchBusinessObjectDataNoFiltersTrimParameters()
    {
        // Create business object data entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        // Search business object data by specifying all business object data search key string parameters with leading and trailing empty spaces.
        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE,
            new BusinessObjectDataSearchRequest(Collections.singletonList(new BusinessObjectDataSearchFilter(Collections.singletonList(
                new BusinessObjectDataSearchKey(addWhitespace(NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(FORMAT_USAGE_CODE),
                    addWhitespace(FORMAT_FILE_TYPE_CODE), FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS, NO_REGISTRATION_DATE_RANGE_FILTER,
                    NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION))))));

        // Validate the results.
        assertEquals(1, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());
        for (BusinessObjectData businessObjectData : result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements())
        {
            assertEquals(NAMESPACE, businessObjectData.getNamespace());
            assertEquals(BDEF_NAME, businessObjectData.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_USAGE_CODE, businessObjectData.getBusinessObjectFormatUsage());
            assertEquals(FORMAT_FILE_TYPE_CODE, businessObjectData.getBusinessObjectFormatFileType());
            assertEquals(FORMAT_VERSION, Integer.valueOf(businessObjectData.getBusinessObjectFormatVersion()));
        }

        // Validate the paging information.
        assertEquals(Long.valueOf(DEFAULT_PAGE_NUMBER), result.getPageNum());
        assertEquals(Long.valueOf(PAGE_SIZE), result.getPageSize());
        assertEquals(Long.valueOf(1), result.getPageCount());
        assertEquals(Long.valueOf(1), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(1), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
    }

    @Test
    public void testSearchBusinessObjectDataNoFiltersUpperCaseParameters()
    {
        // Create business object data entities required for testing.
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        // Search business object data by specifying all business object data search key parameters in uppercase.
        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE,
            new BusinessObjectDataSearchRequest(Collections.singletonList(new BusinessObjectDataSearchFilter(Collections.singletonList(
                new BusinessObjectDataSearchKey(NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), FORMAT_USAGE_CODE.toUpperCase(),
                    FORMAT_FILE_TYPE_CODE.toUpperCase(), FORMAT_VERSION, NO_PARTITION_VALUE_FILTERS, NO_REGISTRATION_DATE_RANGE_FILTER,
                    NO_ATTRIBUTE_VALUE_FILTERS, NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION))))));

        // Validate the results.
        assertEquals(1, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());
        for (BusinessObjectData businessObjectData : result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements())
        {
            assertEquals(NAMESPACE, businessObjectData.getNamespace());
            assertEquals(BDEF_NAME, businessObjectData.getBusinessObjectDefinitionName());
            assertEquals(FORMAT_USAGE_CODE, businessObjectData.getBusinessObjectFormatUsage());
            assertEquals(FORMAT_FILE_TYPE_CODE, businessObjectData.getBusinessObjectFormatFileType());
            assertEquals(FORMAT_VERSION, Integer.valueOf(businessObjectData.getBusinessObjectFormatVersion()));
        }

        // Validate the paging information.
        assertEquals(Long.valueOf(DEFAULT_PAGE_NUMBER), result.getPageNum());
        assertEquals(Long.valueOf(PAGE_SIZE), result.getPageSize());
        assertEquals(Long.valueOf(1), result.getPageCount());
        assertEquals(Long.valueOf(1), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(1), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
    }

    @Test
    public void testSearchBusinessObjectDataPartitionValueFilters()
    {
        // Define a set of partition columns to be used across multiple formats. Where columns 3 and 4 do not have consistent position (change places
        // or even completely replaced with new columns). We will use columns 1, 2, and 5 to performed "optimized" business object data search
        // and columns 3 and 4 to execute "old"/"unoptimized" search logic.
        List<List<String>> partitionKeys = new ArrayList<>();

        // FORMAT_USAGE_CODE and FORMAT_FILE_TYPE_CODE
        // * all partition columns are present and do not change partition levels
        partitionKeys.add(Arrays.asList("Col1", "Col2", "OldCol3", "OldCol4", "Col5"));
        partitionKeys.add(Arrays.asList("Col1", "Col2", "OldCol3", "OldCol4", "Col5"));

        // FORMAT_USAGE_CODE and FORMAT_FILE_TYPE_CODE_2
        // * all partition columns are present and some partition columns are changing their partition levels
        partitionKeys.add(Arrays.asList("Col1", "Col2", "OldCol3", "OldCol4", "Col5"));
        partitionKeys.add(Arrays.asList("Col1", "Col2", "OldCol4", "OldCol3", "Col5"));

        // FORMAT_USAGE_CODE_2 and FORMAT_FILE_TYPE_CODE
        // * not all partition columns are present in every business object format
        partitionKeys.add(Arrays.asList("Col1", "Col2", "OldCol3", "NewCol4", "Col5"));
        partitionKeys.add(Arrays.asList("Col1", "Col2", "NewCol3", "OldCol4", "Col5"));

        // FORMAT_USAGE_CODE_2 and FORMAT_FILE_TYPE_CODE_2
        // * all partition columns are present and do not change partition levels
        partitionKeys.add(Arrays.asList("Col1", "Col2", "OldCol3", "OldCol4", "Col5"));
        partitionKeys.add(Arrays.asList("Col1", "Col2", "OldCol3", "OldCol4", "Col5"));

        // Declare a set of partition values to be used to register business object data with the same set of partition values for all test formats.
        List<String> universalPartitionValues = Arrays.asList("Val1", "Val2", "Val3", "Val4", "Val5");

        // Declare a set of partition values to be used to register business object data with distinct sets of partition values for all test formats.
        List<List<String>> distinctPartitionValues = Lists.newArrayList();
        distinctPartitionValues.add(Arrays.asList("D11", "D21", "D31", "D41", "D51"));
        distinctPartitionValues.add(Arrays.asList("D12", "D22", "D32", "D42", "D52"));
        distinctPartitionValues.add(Arrays.asList("D13", "D23", "D33", "D43", "D53"));
        distinctPartitionValues.add(Arrays.asList("D14", "D24", "D34", "D44", "D54"));
        distinctPartitionValues.add(Arrays.asList("D15", "D25", "D35", "D45", "D55"));
        distinctPartitionValues.add(Arrays.asList("D16", "D26", "D36", "D46", "D56"));
        distinctPartitionValues.add(Arrays.asList("D17", "D27", "D37", "D47", "D57"));
        distinctPartitionValues.add(Arrays.asList("D18", "D28", "D38", "D48", "D58"));

        // Track business object format getting created.
        List<BusinessObjectFormatEntity> businessObjectFormatEntities = new ArrayList<>();

        // Create business object formats using multiple usage, file type, and version values for the list of partition keys declared above.
        for (String businessObjectFormatUsage : Arrays.asList(FORMAT_USAGE_CODE, FORMAT_USAGE_CODE_2))
        {
            for (String fileType : Arrays.asList(FORMAT_FILE_TYPE_CODE, FORMAT_FILE_TYPE_CODE_2))
            {
                for (Integer businessObjectFormatVersion : Arrays.asList(SECOND_FORMAT_VERSION, INITIAL_FORMAT_VERSION))
                {
                    // Create business object format.
                    BusinessObjectFormatEntity businessObjectFormatEntity =
                        businessObjectFormatDaoTestHelper.createBusinessObjectFormatEntity(NAMESPACE, BDEF_NAME, businessObjectFormatUsage, fileType,
                            businessObjectFormatVersion, FORMAT_DESCRIPTION, NO_FORMAT_DOCUMENT_SCHEMA, NO_FORMAT_DOCUMENT_SCHEMA_URL,
                            Objects.equals(businessObjectFormatVersion, SECOND_FORMAT_VERSION) ? LATEST_VERSION_FLAG_SET : NO_LATEST_VERSION_FLAG_SET,
                            partitionKeys.get(businessObjectFormatEntities.size()).get(0), NO_PARTITION_KEY_GROUP, NO_ATTRIBUTES, SCHEMA_DELIMITER_PIPE,
                            SCHEMA_COLLECTION_ITEMS_DELIMITER_COMMA, SCHEMA_MAP_KEYS_DELIMITER_HASH, SCHEMA_ESCAPE_CHARACTER_BACKSLASH,
                            SCHEMA_CUSTOM_ROW_FORMAT, SCHEMA_CUSTOM_CLUSTERED_BY_VALUE, NO_SCHEMA_CUSTOM_TBL_PROPERTIES, SCHEMA_NULL_VALUE_BACKSLASH_N,
                            NO_COLUMNS, businessObjectFormatDaoTestHelper.getSchemaColumns(partitionKeys.get(businessObjectFormatEntities.size())));

                    // Add created business object format to the list.
                    businessObjectFormatEntities.add(businessObjectFormatEntity);
                }
            }
        }

        // Create business object data status entity.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDaoTestHelper.createBusinessObjectDataStatusEntity(BDATA_STATUS);

        // For each format created above, register business object data that has constant set of partition values across all formats
        // and business object data that has distinct set of partition values for each format.
        List<BusinessObjectDataEntity> universalBusinessObjectDataEntities = new ArrayList<>();
        List<BusinessObjectDataEntity> distinctBusinessObjectDataEntities = new ArrayList<>();
        List<BusinessObjectDataEntity> allBusinessObjectDataEntities = new ArrayList<>();
        for (BusinessObjectFormatEntity businessObjectFormatEntity : businessObjectFormatEntities)
        {
            BusinessObjectDataEntity universalBusinessObjectDataEntity =
                businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectFormatEntity, universalPartitionValues.get(0),
                    universalPartitionValues.subList(1, universalPartitionValues.size()), INITIAL_DATA_VERSION, LATEST_VERSION_FLAG_SET,
                    businessObjectDataStatusEntity);
            universalBusinessObjectDataEntities.add(universalBusinessObjectDataEntity);
            allBusinessObjectDataEntities.add(universalBusinessObjectDataEntity);

            BusinessObjectDataEntity distinctBusinessObjectDataEntity =
                businessObjectDataDaoTestHelper.createBusinessObjectDataEntity(businessObjectFormatEntity,
                    distinctPartitionValues.get(distinctBusinessObjectDataEntities.size()).get(0),
                    distinctPartitionValues.get(distinctBusinessObjectDataEntities.size()).subList(1, universalPartitionValues.size()), INITIAL_DATA_VERSION,
                    LATEST_VERSION_FLAG_SET, businessObjectDataStatusEntity);
            distinctBusinessObjectDataEntities.add(distinctBusinessObjectDataEntity);
            allBusinessObjectDataEntities.add(distinctBusinessObjectDataEntity);
        }

        // Create business object data search request without a list of business object data search keys.
        BusinessObjectDataSearchRequest businessObjectDataSearchRequest =
            new BusinessObjectDataSearchRequest(Collections.singletonList(new BusinessObjectDataSearchFilter(null)));

        // Declare variables to be reused in the test calls below.
        BusinessObjectDataSearchKey businessObjectDataSearchKey;
        List<PartitionValueFilter> partitionValueFilters;
        BusinessObjectDataSearchResultPagingInfoDto results;

        // Test business object data search with only required parameters and without any search key filters.
        businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKey.setPartitionValueFilters(NO_PARTITION_VALUE_FILTERS);
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0)
            .setBusinessObjectDataSearchKeys(Collections.singletonList(businessObjectDataSearchKey));

        // Execute the search and validate the results.
        // We expect result set to contain all business object data registered for this business object definition.
        results = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(businessObjectDataDaoTestHelper.getExpectedSearchResults(allBusinessObjectDataEntities),
            results.getBusinessObjectDataSearchResult().getBusinessObjectDataElements());

        // Test business object data search with all parameters and with partition value filters for all partition keys using universal values.
        businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKey.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        businessObjectDataSearchKey.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        businessObjectDataSearchKey.setBusinessObjectFormatVersion(SECOND_FORMAT_VERSION);
        partitionValueFilters = new ArrayList<>();
        partitionValueFilters.add(new PartitionValueFilter("Col1", Lists.newArrayList("Val1"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(new PartitionValueFilter("Col2", Lists.newArrayList("Val2"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(new PartitionValueFilter("OldCol3", Lists.newArrayList("Val3"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(new PartitionValueFilter("OldCol4", Lists.newArrayList("Val4"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(new PartitionValueFilter("Col5", Lists.newArrayList("Val5"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0)
            .setBusinessObjectDataSearchKeys(Collections.singletonList(businessObjectDataSearchKey));

        // Execute the search and validate the results. We expect result set to contain only the first universal business object data from the list.
        results = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(businessObjectDataDaoTestHelper.getExpectedSearchResults(universalBusinessObjectDataEntities.subList(0, 1)),
            results.getBusinessObjectDataSearchResult().getBusinessObjectDataElements());

        // Test business object data search with only required parameters and with partition value filters for all partition keys using universal values.
        businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        partitionValueFilters = new ArrayList<>();
        partitionValueFilters.add(new PartitionValueFilter("Col1", Lists.newArrayList("Val1"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(new PartitionValueFilter("Col2", Lists.newArrayList("Val2"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(new PartitionValueFilter("OldCol3", Lists.newArrayList("Val3"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(new PartitionValueFilter("OldCol4", Lists.newArrayList("Val4"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(new PartitionValueFilter("Col5", Lists.newArrayList("Val5"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0)
            .setBusinessObjectDataSearchKeys(Collections.singletonList(businessObjectDataSearchKey));

        // Execute the search and validate the results. We expect result set to contain universal business object data that matches partition keys from
        // the first business object format. No data expected to be grabbed for formats with swapped OldCol3 and OldCol4 due to partition values
        // matching exact original column positions.
        results = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(businessObjectDataDaoTestHelper.getExpectedSearchResults(
                Arrays.asList(universalBusinessObjectDataEntities.get(0), universalBusinessObjectDataEntities.get(1), universalBusinessObjectDataEntities.get(2),
                    universalBusinessObjectDataEntities.get(6), universalBusinessObjectDataEntities.get(7))),
            results.getBusinessObjectDataSearchResult().getBusinessObjectDataElements());

        // Test business object data search with only required parameters and with partition value filters for all partition keys using universal values.
        // For this test, we want to capture results for formats with swapped OldCol3 and OldCol4, so for those columns we use a list with partition
        // values to match both possible locations.
        businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        partitionValueFilters = new ArrayList<>();
        partitionValueFilters.add(new PartitionValueFilter("Col1", Lists.newArrayList("Val1"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(new PartitionValueFilter("Col2", Lists.newArrayList("Val2"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(
            new PartitionValueFilter("OldCol3", Lists.newArrayList("Val3", "Val4"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(
            new PartitionValueFilter("OldCol4", Lists.newArrayList("Val3", "Val4"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE));
        partitionValueFilters.add(new PartitionValueFilter("Col5", Lists.newArrayList("Val5"), NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0)
            .setBusinessObjectDataSearchKeys(Collections.singletonList(businessObjectDataSearchKey));

        // Execute the search and validate the results. We expect result set to contain universal business object data that matches partition keys from
        // the first business object format along with data for formats with swapped OldCol3 and OldCol4 due to extra partition values passed for those
        // partition keys to cover the swap.
        results = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(businessObjectDataDaoTestHelper.getExpectedSearchResults(
                Arrays.asList(universalBusinessObjectDataEntities.get(0), universalBusinessObjectDataEntities.get(1), universalBusinessObjectDataEntities.get(2),
                    universalBusinessObjectDataEntities.get(3), universalBusinessObjectDataEntities.get(6), universalBusinessObjectDataEntities.get(7))),
            results.getBusinessObjectDataSearchResult().getBusinessObjectDataElements());

        // Test business object data search with only required parameters and with partition value filter with a range for partition key number 1 using
        // distinct partition values to cover a subset distinct business object data.
        businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        partitionValueFilters = new ArrayList<>();
        partitionValueFilters.add(new PartitionValueFilter("Col1", NO_PARTITION_VALUES, new PartitionValueRange("D13", "D17"), NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0)
            .setBusinessObjectDataSearchKeys(Collections.singletonList(businessObjectDataSearchKey));

        // Execute the search and validate the results. We expect result set to contain a specific subset of distinct business object data.
        results = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(businessObjectDataDaoTestHelper.getExpectedSearchResults(distinctBusinessObjectDataEntities.subList(2, 7)),
            results.getBusinessObjectDataSearchResult().getBusinessObjectDataElements());

        // Test business object data search with only required parameters and with partition value filter with a range for partition key number 1 using
        // relative universal partition value.
        businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        partitionValueFilters = new ArrayList<>();
        partitionValueFilters.add(
            new PartitionValueFilter("Col1", NO_PARTITION_VALUES, new PartitionValueRange("Val1", "Val1"), NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE));
        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0)
            .setBusinessObjectDataSearchKeys(Collections.singletonList(businessObjectDataSearchKey));

        // Execute the search and validate the results. We expect result set to contain all universal business object data.
        results = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(businessObjectDataDaoTestHelper.getExpectedSearchResults(universalBusinessObjectDataEntities),
            results.getBusinessObjectDataSearchResult().getBusinessObjectDataElements());

        // Test business object data search with only required parameters and with partition value filter with a range for partition key number 2 using
        // distinct values to cover a subset distinct business object data.
        businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        partitionValueFilters = new ArrayList<>();
        partitionValueFilters.add(new PartitionValueFilter("Col2", NO_PARTITION_VALUES, new PartitionValueRange("D24", "D26"), NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0)
            .setBusinessObjectDataSearchKeys(Collections.singletonList(businessObjectDataSearchKey));

        // Execute the search and validate the results. We expect result set to contain a specific subset of distinct business object data.
        results = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(businessObjectDataDaoTestHelper.getExpectedSearchResults(distinctBusinessObjectDataEntities.subList(3, 6)),
            results.getBusinessObjectDataSearchResult().getBusinessObjectDataElements());

        // Test business object data search with only required parameters and with partition value filter with a range for partition key number 2 using
        // relative universal partition value.
        businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        partitionValueFilters = new ArrayList<>();
        partitionValueFilters.add(
            new PartitionValueFilter("Col2", NO_PARTITION_VALUES, new PartitionValueRange("Val2", "Val2"), NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE));
        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0)
            .setBusinessObjectDataSearchKeys(Collections.singletonList(businessObjectDataSearchKey));

        // Execute the search and validate the results. We expect result set to contain all universal business object data.
        results = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(businessObjectDataDaoTestHelper.getExpectedSearchResults(universalBusinessObjectDataEntities),
            results.getBusinessObjectDataSearchResult().getBusinessObjectDataElements());

        // Test business object data search with only required parameters and with partition value filter with a range for partition key number 3 using
        // relative universal partition values to cover the column swap.
        businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        partitionValueFilters = new ArrayList<>();
        partitionValueFilters.add(
            new PartitionValueFilter("OldCol3", NO_PARTITION_VALUES, new PartitionValueRange("Val3", "Val4"), NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE));
        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0)
            .setBusinessObjectDataSearchKeys(Collections.singletonList(businessObjectDataSearchKey));

        // Execute the search and validate the results. We expect result set to contain universal business object data from format that contain original
        // partition key 3 along with the format that has that key at another position since we provided range that covers both swapped values.
        results = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(businessObjectDataDaoTestHelper.getExpectedSearchResults(
            Arrays.asList(universalBusinessObjectDataEntities.get(0), universalBusinessObjectDataEntities.get(1), universalBusinessObjectDataEntities.get(2),
                universalBusinessObjectDataEntities.get(3), universalBusinessObjectDataEntities.get(4), universalBusinessObjectDataEntities.get(6),
                universalBusinessObjectDataEntities.get(7))), results.getBusinessObjectDataSearchResult().getBusinessObjectDataElements());

        // Test business object data search with only required parameters and with partition value filter with a range for partition key number 4 using
        // relative universal partition values to cover the column swap.
        businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        partitionValueFilters = new ArrayList<>();
        partitionValueFilters.add(
            new PartitionValueFilter("OldCol4", NO_PARTITION_VALUES, new PartitionValueRange("Val3", "Val4"), NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE));
        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0)
            .setBusinessObjectDataSearchKeys(Collections.singletonList(businessObjectDataSearchKey));

        // Execute the search and validate the results. We expect result set to contain universal business object data from format that contain original
        // partition key 4 along with the format that has that key at another position since we provided range that covers both swapped values.
        results = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(businessObjectDataDaoTestHelper.getExpectedSearchResults(
            Arrays.asList(universalBusinessObjectDataEntities.get(0), universalBusinessObjectDataEntities.get(1), universalBusinessObjectDataEntities.get(2),
                universalBusinessObjectDataEntities.get(3), universalBusinessObjectDataEntities.get(5), universalBusinessObjectDataEntities.get(6),
                universalBusinessObjectDataEntities.get(7))), results.getBusinessObjectDataSearchResult().getBusinessObjectDataElements());

        // Test business object data search with only required parameters and with partition value filter with a range for partition key number 5 using
        // distinct values to cover a subset distinct business object data.
        businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        partitionValueFilters = new ArrayList<>();
        partitionValueFilters.add(new PartitionValueFilter("Col5", NO_PARTITION_VALUES, new PartitionValueRange("D52", "D56"), NO_LATEST_BEFORE_PARTITION_VALUE,
            NO_LATEST_AFTER_PARTITION_VALUE));
        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0)
            .setBusinessObjectDataSearchKeys(Collections.singletonList(businessObjectDataSearchKey));

        // Execute the search and validate the results. We expect result set to contain a specific subset of distinct business object data.
        results = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(businessObjectDataDaoTestHelper.getExpectedSearchResults(distinctBusinessObjectDataEntities.subList(1, 6)),
            results.getBusinessObjectDataSearchResult().getBusinessObjectDataElements());

        // Test business object data search with only required parameters and with partition value filter with a range for partition key number 5 using
        // relative universal partition value.
        businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        partitionValueFilters = new ArrayList<>();
        partitionValueFilters.add(
            new PartitionValueFilter("Col5", NO_PARTITION_VALUES, new PartitionValueRange("Val5", "Val5"), NO_LATEST_BEFORE_PARTITION_VALUE,
                NO_LATEST_AFTER_PARTITION_VALUE));
        businessObjectDataSearchKey.setPartitionValueFilters(partitionValueFilters);
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0)
            .setBusinessObjectDataSearchKeys(Collections.singletonList(businessObjectDataSearchKey));

        // Execute the search and validate the results. We expect result set to contain all universal business object data.
        results = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, DEFAULT_PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(businessObjectDataDaoTestHelper.getExpectedSearchResults(universalBusinessObjectDataEntities),
            results.getBusinessObjectDataSearchResult().getBusinessObjectDataElements());
    }

    @Test
    public void testSearchBusinessObjectDataPartitionValueFiltersWithOptimization()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        key.setPartitionValueFilters(partitionValueFilters);
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setPartitionKey(PARTITION_KEY);
        partitionValueFilter.setPartitionValues(Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2));

        businessObjectDataSearchKeys.add(key);
        filters.add(new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys));
        request.setBusinessObjectDataSearchFilters(filters);

        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, request);

        // The search results expect to contain two business object data instances.
        assertEquals(2, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        // Validate the paging information.
        assertEquals(Long.valueOf(DEFAULT_PAGE_NUMBER), result.getPageNum());
        assertEquals(Long.valueOf(PAGE_SIZE), result.getPageSize());
        assertEquals(Long.valueOf(1), result.getPageCount());
        assertEquals(Long.valueOf(2), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(2), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
    }

    @Test
    public void testSearchBusinessObjectDataPartitionValueFiltersBusinessObjectDefinitionNoExists()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(I_DO_NOT_EXIST);

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        key.setPartitionValueFilters(partitionValueFilters);
        PartitionValueFilter partitionValueFilter = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilter);
        partitionValueFilter.setPartitionKey(PARTITION_KEY);
        partitionValueFilter.setPartitionValues(Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2));

        businessObjectDataSearchKeys.add(key);
        filters.add(new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys));
        request.setBusinessObjectDataSearchFilters(filters);

        BusinessObjectDataSearchResultPagingInfoDto result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, request);

        // The result list should be empty.
        assertEquals(0, result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements().size());

        // Validate the paging information.
        assertEquals(Long.valueOf(DEFAULT_PAGE_NUMBER), result.getPageNum());
        assertEquals(Long.valueOf(PAGE_SIZE), result.getPageSize());
        assertEquals(Long.valueOf(0), result.getPageCount());
        assertEquals(Long.valueOf(0), result.getTotalRecordsOnPage());
        assertEquals(Long.valueOf(0), result.getTotalRecordCount());
        assertEquals(Long.valueOf(DEFAULT_PAGE_SIZE), result.getMaxResultsPerPage());
    }

    @Test
    public void testSearchBusinessObjectDataPartitionValueFiltersInvalidPartitionKey()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);
        key.setBusinessObjectFormatUsage(FORMAT_USAGE_CODE);
        key.setBusinessObjectFormatFileType(FORMAT_FILE_TYPE_CODE);
        key.setBusinessObjectFormatVersion(FORMAT_VERSION);

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        key.setPartitionValueFilters(partitionValueFilters);
        PartitionValueFilter partitionValueFilterA = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilterA);
        partitionValueFilterA.setPartitionKey(INVALID_VALUE);
        partitionValueFilterA.setPartitionValues(Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2));
        PartitionValueFilter partitionValueFilterB = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilterB);
        partitionValueFilterB.setPartitionKey(INVALID_VALUE_2);
        partitionValueFilterB.setPartitionValues(Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2));

        businessObjectDataSearchKeys.add(key);
        filters.add(new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys));
        request.setBusinessObjectDataSearchFilters(filters);

        try
        {
            businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("There are no registered business object formats with \"%s\" namespace, \"%s\" business object definition name, " +
                    "\"%s\" business object format usage, \"%s\" business object format file type, \"%s\" business object format version " +
                    "that have schema with partition columns matching \"%s, %s\" partition key(s).", NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE,
                FORMAT_VERSION, INVALID_VALUE, INVALID_VALUE_2), e.getMessage());
        }
    }

    @Test
    public void testSearchBusinessObjectDataPartitionValueFiltersInvalidPartitionKeyMissingOptionalParameters()
    {
        businessObjectDataServiceTestHelper.createDatabaseEntitiesForBusinessObjectDataSearchTesting();

        BusinessObjectDataSearchRequest request = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> filters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey key = new BusinessObjectDataSearchKey();
        key.setNamespace(NAMESPACE);
        key.setBusinessObjectDefinitionName(BDEF_NAME);

        List<PartitionValueFilter> partitionValueFilters = new ArrayList<>();
        key.setPartitionValueFilters(partitionValueFilters);
        PartitionValueFilter partitionValueFilterA = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilterA);
        partitionValueFilterA.setPartitionKey(INVALID_VALUE);
        partitionValueFilterA.setPartitionValues(Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2));
        PartitionValueFilter partitionValueFilterB = new PartitionValueFilter();
        partitionValueFilters.add(partitionValueFilterB);
        partitionValueFilterB.setPartitionKey(INVALID_VALUE_2);
        partitionValueFilterB.setPartitionValues(Arrays.asList(PARTITION_VALUE, PARTITION_VALUE_2));

        businessObjectDataSearchKeys.add(key);
        filters.add(new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys));
        request.setBusinessObjectDataSearchFilters(filters);

        try
        {
            businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("There are no registered business object formats with \"%s\" namespace, \"%s\" business object definition name " +
                    "that have schema with partition columns matching \"%s, %s\" partition key(s).", NAMESPACE, BDEF_NAME, INVALID_VALUE, INVALID_VALUE_2),
                e.getMessage());
        }
    }

    @Test
    public void testSearchBusinessObjectDataPartitionValueFiltersMissingRequiredParameters()
    {
        try
        {
            businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, new BusinessObjectDataSearchRequest(Collections.singletonList(
                new BusinessObjectDataSearchFilter(Collections.singletonList(
                    new BusinessObjectDataSearchKey(BDEF_NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, Collections
                        .singletonList(
                            new PartitionValueFilter(NO_PARTITION_KEY, NO_PARTITION_VALUES, NO_PARTITION_VALUE_RANGE, NO_LATEST_BEFORE_PARTITION_VALUE,
                                NO_LATEST_AFTER_PARTITION_VALUE)), NO_REGISTRATION_DATE_RANGE_FILTER, NO_ATTRIBUTE_VALUE_FILTERS,
                        NO_FILTER_ON_LATEST_VALID_VERSION, NO_FILTER_ON_RETENTION_EXPIRATION))))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A partition key must be specified.", e.getMessage());
        }
    }

    @Test
    public void testSearchBusinessObjectDataRegistrationDateRangeFilter() throws DatatypeConfigurationException
    {
        // Create a business object data entity.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoTestHelper
            .createBusinessObjectDataEntity(NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION, PARTITION_VALUE,
                NO_SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID);

        // Set "created on" timestamp for this business object data to a test value, so we can validate filtering on registration date and time.
        Timestamp createdOnTimestamp = Timestamp.valueOf("2016-03-29 10:34:11.311");
        businessObjectDataEntity.setCreatedOn(createdOnTimestamp);
        businessObjectDataDao.saveAndRefresh(businessObjectDataEntity);

        // Create expected business object data search result for the test business object data.
        BusinessObjectData expectedBusinessObjectData =
            new BusinessObjectData(businessObjectDataEntity.getId(), NAMESPACE, BDEF_NAME, FORMAT_USAGE_CODE, FORMAT_FILE_TYPE_CODE, FORMAT_VERSION,
                PARTITION_KEY, PARTITION_VALUE, NULL_AS_SUBPARTITION_VALUES, DATA_VERSION, LATEST_VERSION_FLAG_SET, BusinessObjectDataStatusEntity.VALID,
                NULL_AS_STORAGE_UNITS, NULL_AS_ATTRIBUTES, NULL_AS_BUSINESS_OBJECT_DATA_PARENTS, NULL_AS_BUSINESS_OBJECT_DATA_CHILDREN,
                NO_BUSINESS_OBJECT_DATA_STATUS_HISTORY, NO_RETENTION_EXPIRATION_DATE, NO_CREATED_BY, NO_CREATED_ON);

        // Create business object data search request without any filters.
        BusinessObjectDataSearchRequest businessObjectDataSearchRequest = new BusinessObjectDataSearchRequest();
        List<BusinessObjectDataSearchFilter> businessObjectDataSearchFilters = new ArrayList<>();
        List<BusinessObjectDataSearchKey> businessObjectDataSearchKeys = new ArrayList<>();
        BusinessObjectDataSearchKey businessObjectDataSearchKey = new BusinessObjectDataSearchKey();
        businessObjectDataSearchKey.setNamespace(NAMESPACE);
        businessObjectDataSearchKey.setBusinessObjectDefinitionName(BDEF_NAME);
        businessObjectDataSearchKeys.add(businessObjectDataSearchKey);
        businessObjectDataSearchFilters.add(new BusinessObjectDataSearchFilter(businessObjectDataSearchKeys));
        businessObjectDataSearchRequest.setBusinessObjectDataSearchFilters(businessObjectDataSearchFilters);

        // Declare variables to be used for the search calls validation.
        XMLGregorianCalendar start;
        XMLGregorianCalendar end;
        BusinessObjectDataSearchResultPagingInfoDto result;

        // Validate that our search request matches test business object data.
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(Collections.singletonList(expectedBusinessObjectData), result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements());

        // The test cases below validate the following for the business object data search when it is filtered by registration date-time:
        // 1) Registration date-time filter works on a half-open range: [start datetime, end datetime)
        // 2) The start datetime value with or without time portion is used exactly as specified. ie, if end datetime is specified as 2017-01-02, the value to
        //    filter is 2017-01-02T00:00:00
        // 3) If no time portion is specified in the end datetime value then the date is assumed to be the start of next day. ie, if end datetime is specified
        //    as 2017-01-01, the value used to filter is 2017-01-02T00:00:00
        // 4) If time portion is specified in the end datetime value, then the datetime portion it is used exactly as specified (unlike how it is done for date
        //    only end datetime value). ie, if end datetime is specified as 2017-01-02T10:11, the value to filter is 2017-01-02T10:11:00

        // Testing filtering on date without time portion...

        // Start date without time portion is less then createdOn date without time portion.
        // End date is not specified.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-28");
        end = NO_END_REGISTRATION_DATE;
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date without time portion is equal to createdOn date without time portion.
        // End date is not specified.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29");
        end = NO_END_REGISTRATION_DATE;
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date without time portion is greater than createdOn date without time portion.
        // End date is not specified.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-30");
        end = NO_END_REGISTRATION_DATE;
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date is not specified.
        // End date without time portion is less then createdOn date without time portion.
        start = NO_START_REGISTRATION_DATE;
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-28");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date is not specified.
        // End date without time portion is equal to createdOn date without time portion.
        start = NO_START_REGISTRATION_DATE;
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date is not specified.
        // End date without time portion is greater than createdOn date without time portion.
        start = NO_START_REGISTRATION_DATE;
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-30");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date without time portion is less than createdOn date without time portion.
        // End date without time portion is less than createdOn date without time portion.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-28");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-28");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date without time portion is less than createdOn date without time portion.
        // End date without time portion is equal to createdOn date without time portion.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-28");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date without time portion is less than createdOn date without time portion.
        // End date without time portion is greater than createdOn date without time portion.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-28");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-30");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date without time portion is equal to createdOn date without time portion.
        // End date without time portion is equal to createdOn date without time portion.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date without time portion is greater than createdOn date without time portion.
        // End date without time portion is greater than createdOn date without time portion.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-30");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-30");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Testing filtering on date with time portion...

        // Start date with time portion is less then createdOn rounded to seconds.
        // End date is not specified.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:10");
        end = NO_END_REGISTRATION_DATE;
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date with time portion is equal to createdOn rounded to seconds.
        // End date is not specified.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:11");
        end = NO_END_REGISTRATION_DATE;
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date with time portion is equal to createdOn (to the millisecond).
        // End date is not specified.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:11.311");
        end = NO_END_REGISTRATION_DATE;
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date with time portion is greater than createdOn by 1 millisecond.
        // End date is not specified.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:11.312");
        end = NO_END_REGISTRATION_DATE;
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date with time portion is greater than createdOn rounded to seconds.
        // End date is not specified.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:12");
        end = NO_END_REGISTRATION_DATE;
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date is not specified.
        // End date with time portion is less then createdOn rounded to seconds.
        start = NO_START_REGISTRATION_DATE;
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:10");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date is not specified.
        // End date with time portion is equal to createdOn rounded to seconds.
        start = NO_START_REGISTRATION_DATE;
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:11");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date is not specified.
        // End date with time portion is equal to createdOn (to the millisecond).
        start = NO_START_REGISTRATION_DATE;
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:11.311");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date is not specified.
        // End date with time portion is greater than createdOn by 1 millisecond.
        start = NO_START_REGISTRATION_DATE;
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:11.312");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date is not specified.
        // End date with time portion is greater than createdOn rounded to seconds.
        start = NO_START_REGISTRATION_DATE;
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:12");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date with time portion is less then createdOn rounded to seconds.
        // End date with time portion is less then createdOn rounded to seconds.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:10");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:10");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date with time portion is less then createdOn rounded to seconds.
        // End date with time portion is equal to createdOn rounded to seconds.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:10");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:11");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date with time portion is less then createdOn rounded to seconds.
        // End date with time portion is equal to createdOn (to the millisecond).
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:10");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:11.311");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date with time portion is less then createdOn rounded to seconds.
        // End date with time portion is greater than createdOn by 1 millisecond.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:10");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:11.312");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date with time portion is less then createdOn rounded to seconds.
        // End date with time portion is greater than createdOn rounded to seconds.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:10");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:12");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date with time portion is equal to createdOn rounded to seconds.
        // End date with time portion is greater than createdOn rounded to seconds.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:11");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:12");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date with time portion is equal to createdOn (to the millisecond).
        // End date with time portion is greater than createdOn rounded to seconds.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:11.311");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:12");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(1, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date with time portion is greater than createdOn by 1 millisecond.
        // End date with time portion is greater than createdOn rounded to seconds.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:11.312");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:12");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));

        // Start date with time portion is greater than createdOn rounded to seconds.
        // End date with time portion is greater than createdOn rounded to seconds.
        start = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:12");
        end = DatatypeFactory.newInstance().newXMLGregorianCalendar("2016-03-29T10:34:12");
        businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0)
            .setRegistrationDateRangeFilter(new RegistrationDateRangeFilter(start, end));
        result = businessObjectDataService.searchBusinessObjectData(DEFAULT_PAGE_NUMBER, PAGE_SIZE, businessObjectDataSearchRequest);
        assertEquals(0, CollectionUtils.size(result.getBusinessObjectDataSearchResult().getBusinessObjectDataElements()));
    }
}
