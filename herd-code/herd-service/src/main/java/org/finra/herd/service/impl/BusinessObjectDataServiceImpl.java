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
package org.finra.herd.service.impl;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.StorageUnitDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectData;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributesUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailability;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataAvailabilityRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdl;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredResponse;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKeys;
import org.finra.herd.model.api.xml.BusinessObjectDataParentsUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataPartitions;
import org.finra.herd.model.api.xml.BusinessObjectDataPartitionsRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataRetentionInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataRetryStoragePolicyTransitionRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDataSearchResult;
import org.finra.herd.model.api.xml.BusinessObjectDataStatus;
import org.finra.herd.model.api.xml.BusinessObjectDataVersion;
import org.finra.herd.model.api.xml.BusinessObjectDataVersions;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.PartitionValueFilter;
import org.finra.herd.model.dto.BusinessObjectDataDestroyDto;
import org.finra.herd.model.dto.BusinessObjectDataRestoreDto;
import org.finra.herd.model.dto.BusinessObjectDataSearchResultPagingInfoDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.StorageUnitAvailabilityDto;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.CustomDdlEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.RetentionTypeEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.StoragePlatformEntity;
import org.finra.herd.service.BusinessObjectDataInitiateDestroyHelperService;
import org.finra.herd.service.BusinessObjectDataInitiateRestoreHelperService;
import org.finra.herd.service.BusinessObjectDataService;
import org.finra.herd.service.NotificationEventService;
import org.finra.herd.service.helper.AttributeDaoHelper;
import org.finra.herd.service.helper.AttributeHelper;
import org.finra.herd.service.helper.BusinessObjectDataDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDataDdlPartitionsHelper;
import org.finra.herd.service.helper.BusinessObjectDataHelper;
import org.finra.herd.service.helper.BusinessObjectDataInvalidateUnregisteredHelper;
import org.finra.herd.service.helper.BusinessObjectDataPartitionsHelper;
import org.finra.herd.service.helper.BusinessObjectDataRetryStoragePolicyTransitionHelper;
import org.finra.herd.service.helper.BusinessObjectDataSearchHelper;
import org.finra.herd.service.helper.BusinessObjectDataStatusDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.CustomDdlDaoHelper;
import org.finra.herd.service.helper.DdlGeneratorFactory;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.StorageHelper;
import org.finra.herd.service.helper.StorageUnitHelper;

/**
 * The business object data service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectDataServiceImpl implements BusinessObjectDataService
{
    /**
     * The partition key value for business object data without partitioning.
     */
    public static final String NO_PARTITIONING_PARTITION_KEY = "partition";

    /**
     * The partition value for business object data without partitioning.
     */
    public static final String NO_PARTITIONING_PARTITION_VALUE = "none";

    /**
     * A status reason of "not registered".
     */
    public static final String REASON_NOT_REGISTERED = "NOT_REGISTERED";

    @Autowired
    private AttributeDaoHelper attributeDaoHelper;

    @Autowired
    private AttributeHelper attributeHelper;

    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDataDaoHelper businessObjectDataDaoHelper;

    @Autowired
    private BusinessObjectDataHelper businessObjectDataHelper;

    @Autowired
    private BusinessObjectDataDdlPartitionsHelper businessObjectDataDdlPartitionsHelper;

    @Autowired
    private BusinessObjectDataInitiateDestroyHelperService businessObjectDataInitiateDestroyHelperService;

    @Autowired
    private BusinessObjectDataInitiateRestoreHelperService businessObjectDataInitiateRestoreHelperService;

    @Autowired
    private BusinessObjectDataInvalidateUnregisteredHelper businessObjectDataInvalidateUnregisteredHelper;

    @Autowired
    private BusinessObjectDataRetryStoragePolicyTransitionHelper businessObjectDataRetryStoragePolicyTransitionHelper;

    @Autowired
    private BusinessObjectDataSearchHelper businessObjectDataSearchHelper;

    @Autowired
    private BusinessObjectDataStatusDaoHelper businessObjectDataStatusDaoHelper;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private BusinessObjectDataPartitionsHelper businessObjectDataPartitionsHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private CustomDdlDaoHelper customDdlDaoHelper;

    @Autowired
    private DdlGeneratorFactory ddlGeneratorFactory;

    @Autowired
    private NotificationEventService notificationEventService;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private StorageHelper storageHelper;

    @Autowired
    private StorageUnitDao storageUnitDao;

    @Autowired
    private StorageUnitHelper storageUnitHelper;

    @NamespacePermission(fields = "#request.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataAvailability checkBusinessObjectDataAvailability(BusinessObjectDataAvailabilityRequest request)
    {
        return checkBusinessObjectDataAvailabilityImpl(request);
    }

    @NamespacePermission(fields = "#request?.businessObjectDataAvailabilityRequests?.![namespace]",
        permissions = NamespacePermissionEnum.READ)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataAvailabilityCollectionResponse checkBusinessObjectDataAvailabilityCollection(
        BusinessObjectDataAvailabilityCollectionRequest request)
    {
        return checkBusinessObjectDataAvailabilityCollectionImpl(request);
    }

    @PublishNotificationMessages
    @NamespacePermission(fields = "#request.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectData createBusinessObjectData(BusinessObjectDataCreateRequest request)
    {
        return businessObjectDataDaoHelper.createBusinessObjectData(request);
    }

    @NamespacePermission(fields = "#businessObjectDataKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectData deleteBusinessObjectData(BusinessObjectDataKey businessObjectDataKey, Boolean deleteFiles)
    {
        // Return the deleted business object data.
        return businessObjectDataDaoHelper.deleteBusinessObjectData(businessObjectDataKey, deleteFiles);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation executes non-transactionally, suspends the current transaction if one exists.
     */
    @NamespacePermission(fields = "#businessObjectDataKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public BusinessObjectData destroyBusinessObjectData(BusinessObjectDataKey businessObjectDataKey)
    {
        return destroyBusinessObjectDataImpl(businessObjectDataKey);
    }

    @NamespacePermission(fields = "#request.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataDdl generateBusinessObjectDataDdl(BusinessObjectDataDdlRequest request)
    {
        return generateBusinessObjectDataDdlImpl(request, false);
    }

    @NamespacePermission(fields = "#request?.businessObjectDataDdlRequests?.![namespace]", permissions = NamespacePermissionEnum.READ)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataDdlCollectionResponse generateBusinessObjectDataDdlCollection(BusinessObjectDataDdlCollectionRequest request)
    {
        return generateBusinessObjectDataDdlCollectionImpl(request);
    }

    @NamespacePermission(fields = "#businessObjectDefinitionKey.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public BusinessObjectDataKeys getAllBusinessObjectDataByBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        // Perform validation and trim.
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);

        // Ensure that a business object definition already exists with the specified name.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Get the maximum number of records to return.
        Integer maxResults = configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DATA_GET_ALL_MAX_RESULT_COUNT, Integer.class);

        // Gets the list of keys and return them.
        BusinessObjectDataKeys businessObjectDataKeys = new BusinessObjectDataKeys();
        businessObjectDataKeys.getBusinessObjectDataKeys()
            .addAll(businessObjectDataDao.getBusinessObjectDataByBusinessObjectDefinition(businessObjectDefinitionEntity, maxResults));
        return businessObjectDataKeys;
    }

    @NamespacePermission(fields = "#businessObjectFormatKey.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public BusinessObjectDataKeys getAllBusinessObjectDataByBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Perform validation and trim. Please note that we specify business object format version parameter to be required.
        businessObjectFormatHelper.validateBusinessObjectFormatKey(businessObjectFormatKey, true);

        // Ensure that a business object definition already exists with the specified name.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);

        // Get the maximum number of records to return.
        Integer maxResults = configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DATA_GET_ALL_MAX_RESULT_COUNT, Integer.class);

        // Gets the list of business object data keys and return them.
        BusinessObjectDataKeys businessObjectDataKeys = new BusinessObjectDataKeys();
        businessObjectDataKeys.getBusinessObjectDataKeys()
            .addAll(businessObjectDataDao.getBusinessObjectDataByBusinessObjectFormat(businessObjectFormatEntity, maxResults));
        return businessObjectDataKeys;
    }

    @NamespacePermission(fields = "#businessObjectDataKey.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectData getBusinessObjectData(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey,
        String businessObjectDataStatus, Boolean includeBusinessObjectDataStatusHistory, Boolean includeStorageUnitStatusHistory,
        Boolean excludeBusinessObjectDataStorageFiles)
    {
        return getBusinessObjectDataImpl(businessObjectDataKey, businessObjectFormatPartitionKey, businessObjectDataStatus,
            includeBusinessObjectDataStatusHistory, includeStorageUnitStatusHistory, excludeBusinessObjectDataStorageFiles);
    }

    @NamespacePermission(fields = "#businessObjectDataKey.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    public BusinessObjectDataVersions getBusinessObjectDataVersions(BusinessObjectDataKey businessObjectDataKey)
    {
        // Validate and trim the business object data key.
        businessObjectDataHelper.validateBusinessObjectDataKey(businessObjectDataKey, false, false);

        // Get the business object data versions based on the specified parameters.
        List<BusinessObjectDataEntity> businessObjectDataEntities = businessObjectDataDao.getBusinessObjectDataEntities(businessObjectDataKey);

        // Create the response.
        BusinessObjectDataVersions businessObjectDataVersions = new BusinessObjectDataVersions();
        for (BusinessObjectDataEntity businessObjectDataEntity : businessObjectDataEntities)
        {
            BusinessObjectDataVersion businessObjectDataVersion = new BusinessObjectDataVersion();
            BusinessObjectDataKey businessObjectDataVersionKey = businessObjectDataHelper.getBusinessObjectDataKey(businessObjectDataEntity);
            businessObjectDataVersion.setBusinessObjectDataKey(businessObjectDataVersionKey);
            businessObjectDataVersion.setStatus(businessObjectDataEntity.getStatus().getCode());
            businessObjectDataVersions.getBusinessObjectDataVersions().add(businessObjectDataVersion);
        }

        return businessObjectDataVersions;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Delegates implementation to {@link org.finra.herd.service.helper.BusinessObjectDataInvalidateUnregisteredHelper}. Starts a new transaction. Meant for
     * Activiti wrapper usage.
     */
    @PublishNotificationMessages
    @NamespacePermission(fields = "#businessObjectDataInvalidateUnregisteredRequest.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataInvalidateUnregisteredResponse invalidateUnregisteredBusinessObjectData(
        BusinessObjectDataInvalidateUnregisteredRequest businessObjectDataInvalidateUnregisteredRequest)
    {
        return invalidateUnregisteredBusinessObjectDataImpl(businessObjectDataInvalidateUnregisteredRequest);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * This implementation executes non-transactionally, suspends the current transaction if one exists.
     */
    @NamespacePermission(fields = "#businessObjectDataKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public BusinessObjectData restoreBusinessObjectData(BusinessObjectDataKey businessObjectDataKey, Integer expirationInDays, String archiveRetrievalOption)
    {
        return restoreBusinessObjectDataImpl(businessObjectDataKey, expirationInDays, archiveRetrievalOption);
    }

    @NamespacePermission(fields = "#request.namespace", permissions = NamespacePermissionEnum.READ)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDataPartitions generateBusinessObjectDataPartitions(BusinessObjectDataPartitionsRequest request)
    {
        return generateBusinessObjectDataPartitionsImpl(request, false);
    }

    @NamespacePermission(fields = "#businessObjectDataKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectData retryStoragePolicyTransition(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataRetryStoragePolicyTransitionRequest request)
    {
        return businessObjectDataRetryStoragePolicyTransitionHelper.retryStoragePolicyTransition(businessObjectDataKey, request);
    }

    @NamespacePermission(fields = "#businessObjectDataSearchRequest.businessObjectDataSearchFilters[0].BusinessObjectDataSearchKeys[0].namespace",
        permissions = NamespacePermissionEnum.READ)
    @Override
    public BusinessObjectDataSearchResultPagingInfoDto searchBusinessObjectData(Integer pageNum, Integer pageSize,
        BusinessObjectDataSearchRequest businessObjectDataSearchRequest)
    {
        // TODO: Check name space permission for all entries in the request.
        // Validate the business object data search request.
        businessObjectDataSearchHelper.validateBusinessObjectDataSearchRequest(businessObjectDataSearchRequest);

        // Get the maximum number of results that can be returned on any page of data. The "pageSize" query parameter should not be greater than
        // this value or an HTTP status of 400 (Bad Request) error would be returned.
        int maxResultsPerPage = configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_MAX_PAGE_SIZE, Integer.class);

        // Get the maximum number of results that can be returned by the search query when selecting raw data to filter in latest valid versions.
        final int rawSearchPageSize = configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_QUERY_PAGINATION_SIZE, Integer.class);

        // Validate the page number and page size
        // Set the defaults if pageNum and pageSize are null
        // Page number must be greater than 0
        // Page size must be greater than 0 and less than maximum page size
        pageNum = businessObjectDataSearchHelper.validatePagingParameter("pageNum", pageNum, 1, Integer.MAX_VALUE);
        pageSize = businessObjectDataSearchHelper.validatePagingParameter("pageSize", pageSize, maxResultsPerPage, maxResultsPerPage);

        // Get the maximum record count that is configured in the system.
        Integer businessObjectDataSearchMaxResultCount =
            configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DATA_SEARCH_MAX_RESULT_COUNT, Integer.class);

        // Get the business object data search key.
        // We assume that the input list contains only one filter with a single search key, since validation should be passed by now.
        BusinessObjectDataSearchKey businessObjectDataSearchKey =
            businessObjectDataSearchRequest.getBusinessObjectDataSearchFilters().get(0).getBusinessObjectDataSearchKeys().get(0);

        // Validate partition keys in partition value filters.
        if (CollectionUtils.isNotEmpty(businessObjectDataSearchKey.getPartitionValueFilters()))
        {
            // Get a count of business object formats that match the business object data search key parameters without the list of partition keys.
            Long businessObjectFormatRecordCount = businessObjectFormatDao
                .getBusinessObjectFormatCountByPartitionKeys(businessObjectDataSearchKey.getNamespace(),
                    businessObjectDataSearchKey.getBusinessObjectDefinitionName(), businessObjectDataSearchKey.getBusinessObjectFormatUsage(),
                    businessObjectDataSearchKey.getBusinessObjectFormatFileType(), businessObjectDataSearchKey.getBusinessObjectFormatVersion(), null);

            // If business object format record count is zero, we return an empty result list.
            if (businessObjectFormatRecordCount == 0)
            {
                return new BusinessObjectDataSearchResultPagingInfoDto(pageNum.longValue(), pageSize.longValue(), 0L, 0L, 0L, (long) maxResultsPerPage,
                    new BusinessObjectDataSearchResult(new ArrayList<>()));
            }

            // Get partition keys from the list of partition value filters.
            List<String> partitionKeys = new ArrayList<>();
            for (PartitionValueFilter partitionValueFilter : businessObjectDataSearchKey.getPartitionValueFilters())
            {
                // Get partition key from the partition value filter. Partition key should not be empty, since validation is passed by now.
                partitionKeys.add(partitionValueFilter.getPartitionKey());
            }

            // Get a count of business object formats that match the business object data search key parameters and the list of partition keys.
            businessObjectFormatRecordCount = businessObjectFormatDao.getBusinessObjectFormatCountByPartitionKeys(businessObjectDataSearchKey.getNamespace(),
                businessObjectDataSearchKey.getBusinessObjectDefinitionName(), businessObjectDataSearchKey.getBusinessObjectFormatUsage(),
                businessObjectDataSearchKey.getBusinessObjectFormatFileType(), businessObjectDataSearchKey.getBusinessObjectFormatVersion(), partitionKeys);

            // Fail if business object formats found that contain specified partition keys in their schema.
            Assert.isTrue(businessObjectFormatRecordCount > 0, String
                .format("There are no registered business object formats with \"%s\" namespace, \"%s\" business object definition name",
                    businessObjectDataSearchKey.getNamespace(), businessObjectDataSearchKey.getBusinessObjectDefinitionName()) +
                (StringUtils.isNotBlank(businessObjectDataSearchKey.getBusinessObjectFormatUsage()) ?
                    String.format(", \"%s\" business object format usage", businessObjectDataSearchKey.getBusinessObjectFormatUsage()) : "") +
                (StringUtils.isNotBlank(businessObjectDataSearchKey.getBusinessObjectFormatFileType()) ?
                    String.format(", \"%s\" business object format file type", businessObjectDataSearchKey.getBusinessObjectFormatFileType()) : "") +
                (businessObjectDataSearchKey.getBusinessObjectFormatVersion() != null ?
                    String.format(", \"%d\" business object format version", businessObjectDataSearchKey.getBusinessObjectFormatVersion()) : "") +
                String.format(" that have schema with partition columns matching \"%s\" partition key(s).", String.join(", ", partitionKeys)));
        }

        // Get the total record count up to to the maximum allowed record count that is configured in the system plus one more record.
        Integer totalRecordCount =
            businessObjectDataDao.getBusinessObjectDataLimitedCountBySearchKey(businessObjectDataSearchKey, businessObjectDataSearchMaxResultCount + 1);

        // Validate the total record count.
        if (totalRecordCount > businessObjectDataSearchMaxResultCount)
        {
            throw new IllegalArgumentException(
                String.format("Result limit of %d exceeded. Modify filters to further limit results.", businessObjectDataSearchMaxResultCount));
        }

        // Compute the number of records that we would need to skip due to page number and page size specified in the request.
        // Please note that page numbers are one-based.
        Integer numberOfRecordsToSkip = (pageNum - 1) * pageSize;

        // Get the search results.
        // We only apply restriction on business object data status here. For performance reasons,
        // selection of actual latest valid versions does not take place on the database end.
        List<BusinessObjectData> businessObjectDataList;
        // If total record count is not greater than the number of records that we would have to skip
        // due to page number and page size specified in the request, then return an empty result list.
        if (totalRecordCount <= numberOfRecordsToSkip)
        {
            businessObjectDataList = new ArrayList<>();
        }
        // Otherwise, is latest valid filter is not enabled, execute the search without any special filtering on our side.
        else if (BooleanUtils.isNotTrue(businessObjectDataSearchKey.isFilterOnLatestValidVersion()))
        {
            businessObjectDataList = businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, pageNum, pageSize);
        }
        // Execute search with filtering on the latest valid version for each set of partition values.
        // Please note that VALID status filtering is already done in the main search query.
        else
        {
            // Compute the number of records that we need to get before we can return the search results.
            // Please note that page numbers are one-based.
            Integer maxLatestValidVersionsRequired = pageNum * pageSize;

            // Use linked hash map to filter entries and to preserve the original order of the entries returned by the database while doing it.
            Map<List<String>, BusinessObjectData> latestVersions = new LinkedHashMap<>();

            // Keep fetching in business object data raw search results using business object data search query pagination size configured in the system.
            boolean keepProcessing = true;
            int rawSearchPageNum = 0;
            while (keepProcessing)
            {
                List<BusinessObjectData> rawSearchBusinessObjectDataList =
                    businessObjectDataDao.searchBusinessObjectData(businessObjectDataSearchKey, ++rawSearchPageNum, rawSearchPageSize);

                // Process the list of business object data search results to select the latest versions.
                for (BusinessObjectData businessObjectData : rawSearchBusinessObjectDataList)
                {
                    // Retrieve business object data key and a list of partition values for this business object data.
                    List<String> alternateKeyValues = getBusinessObjectDataVersionLessAlternateKeyValues(businessObjectData);

                    // If hash map already contains this set of version-less alternate key values, that means that we already got the latest valid version
                    // filtered in. This is because of the order by used in the search query, where both business object format and business object data
                    // versions are specified to be in descending order and also business object format usage sorted in upper case.
                    if (!latestVersions.containsKey(alternateKeyValues))
                    {
                        // Insert this latest version into the hash map.
                        latestVersions.put(alternateKeyValues, businessObjectData);

                        // Check if we got enough latest valid versions.
                        if (latestVersions.size() == maxLatestValidVersionsRequired)
                        {
                            keepProcessing = false;
                            break;
                        }
                    }
                }

                // Determine if we need to keep processing.
                if (keepProcessing)
                {
                    keepProcessing = (rawSearchBusinessObjectDataList.size() == rawSearchPageSize);
                }
            }

            // Get the correct subset of search results to be returned in the response.
            businessObjectDataList = new ArrayList<>(latestVersions.values());
            businessObjectDataList = businessObjectDataList.subList(numberOfRecordsToSkip, businessObjectDataList.size());
        }

        // Get the page count.
        Integer pageCount = totalRecordCount / pageSize + (totalRecordCount % pageSize > 0 ? 1 : 0);

        // Build and return the business object data search result with the paging information.
        return new BusinessObjectDataSearchResultPagingInfoDto(pageNum.longValue(), pageSize.longValue(), pageCount.longValue(),
            (long) businessObjectDataList.size(), totalRecordCount.longValue(), (long) maxResultsPerPage,
            new BusinessObjectDataSearchResult(businessObjectDataList));
    }

    @NamespacePermission(fields = "#businessObjectDataKey.namespace", permissions = {NamespacePermissionEnum.WRITE, NamespacePermissionEnum.WRITE_ATTRIBUTE})
    @Override
    public BusinessObjectData updateBusinessObjectDataAttributes(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataAttributesUpdateRequest businessObjectDataAttributesUpdateRequest)
    {
        // Validate and trim the business object data key.
        businessObjectDataHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true);

        // Validate the update request.
        Assert.notNull(businessObjectDataAttributesUpdateRequest, "A business object data attributes update request must be specified.");
        Assert.notNull(businessObjectDataAttributesUpdateRequest.getAttributes(), "A list of business object data attributes must be specified.");
        List<Attribute> attributes = businessObjectDataAttributesUpdateRequest.getAttributes();

        // Validate attributes. This is also going to trim the attribute names.
        attributeHelper.validateAttributes(attributes);

        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Validate attributes against attribute definitions.
        attributeDaoHelper.validateAttributesAgainstBusinessObjectDataAttributeDefinitions(attributes,
            businessObjectDataEntity.getBusinessObjectFormat().getAttributeDefinitions());

        // Update the attributes.
        attributeDaoHelper.updateBusinessObjectDataAttributes(businessObjectDataEntity, attributes);

        // Persist and refresh the entity.
        businessObjectDataEntity = businessObjectDataDao.saveAndRefresh(businessObjectDataEntity);

        // Create and return the business object data object from the persisted entity.
        return businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity);
    }

    @NamespacePermission(fields = "#businessObjectDataKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectData updateBusinessObjectDataParents(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataParentsUpdateRequest businessObjectDataParentsUpdateRequest)
    {
        // Validate and trim the business object data key.
        businessObjectDataHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true);

        // Validate the update request.
        Assert.notNull(businessObjectDataParentsUpdateRequest, "A business object data parents update request must be specified.");

        // Validate and trim the parents' keys.
        businessObjectDataDaoHelper.validateBusinessObjectDataKeys(businessObjectDataParentsUpdateRequest.getBusinessObjectDataParents());

        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Fail if this business object data is not in a pre-registration status.
        if (BooleanUtils.isNotTrue(businessObjectDataEntity.getStatus().getPreRegistrationStatus()))
        {
            throw new IllegalArgumentException(String
                .format("Unable to update parents for business object data because it has \"%s\" status, which is not one of pre-registration statuses.",
                    businessObjectDataEntity.getStatus().getCode()));
        }

        // Update parents.
        List<BusinessObjectDataEntity> businessObjectDataParents = businessObjectDataEntity.getBusinessObjectDataParents();

        // Remove all existing parents.
        businessObjectDataParents.clear();

        // Loop through all business object data parents specified in the request and add them one by one.
        if (CollectionUtils.isNotEmpty(businessObjectDataParentsUpdateRequest.getBusinessObjectDataParents()))
        {
            for (BusinessObjectDataKey businessObjectDataParentKey : businessObjectDataParentsUpdateRequest.getBusinessObjectDataParents())
            {
                // Look up parent business object data.
                BusinessObjectDataEntity businessObjectDataParentEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataParentKey);

                // Add business object data entity being updated as a dependent (i.e. child) of the looked up parent.
                businessObjectDataParentEntity.getBusinessObjectDataChildren().add(businessObjectDataEntity);

                // Add the looked up parent as a parent of the business object data entity being updated.
                businessObjectDataParents.add(businessObjectDataParentEntity);
            }
        }

        // Persist and refresh the entity.
        businessObjectDataEntity = businessObjectDataDao.saveAndRefresh(businessObjectDataEntity);

        // Create and return the business object data object from the persisted entity.
        return businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity);
    }

    @NamespacePermission(fields = "#businessObjectDataKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectData updateBusinessObjectDataRetentionInformation(BusinessObjectDataKey businessObjectDataKey,
        BusinessObjectDataRetentionInformationUpdateRequest businessObjectDataRetentionInformationUpdateRequest)
    {
        // Validate and trim the business object data key.
        businessObjectDataHelper.validateBusinessObjectDataKey(businessObjectDataKey, true, true);

        // Validate the update request.
        Assert.notNull(businessObjectDataRetentionInformationUpdateRequest, "A business object data retention information update request must be specified.");

        // Retrieve the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = businessObjectDataDaoHelper.getBusinessObjectDataEntity(businessObjectDataKey);

        // Get the latest version of the business object format for this business object data.
        BusinessObjectFormatKey businessObjectFormatKey =
            new BusinessObjectFormatKey(businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(), null);
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);

        // Fail if business object format for this business object data does not have
        // retention information configured with BDATA_RETENTION_DATE retention type.
        if (businessObjectFormatEntity.getRetentionType() == null ||
            !businessObjectFormatEntity.getRetentionType().getCode().equals(RetentionTypeEntity.BDATA_RETENTION_DATE))
        {
            throw new IllegalArgumentException(String
                .format("Retention information with %s retention type must be configured for business object format. Business object format: {%s}",
                    RetentionTypeEntity.BDATA_RETENTION_DATE, businessObjectFormatHelper.businessObjectFormatKeyToString(businessObjectFormatKey)));
        }

        // Update the retention information.
        businessObjectDataEntity.setRetentionExpiration(businessObjectDataRetentionInformationUpdateRequest.getRetentionExpirationDate() != null ?
            new Timestamp(businessObjectDataRetentionInformationUpdateRequest.getRetentionExpirationDate().toGregorianCalendar().getTimeInMillis()) : null);

        // Persist and refresh the entity.
        businessObjectDataEntity = businessObjectDataDao.saveAndRefresh(businessObjectDataEntity);

        // Create and return the business object data object from the persisted entity.
        return businessObjectDataHelper.createBusinessObjectDataFromEntity(businessObjectDataEntity);
    }

    /**
     * Performs an availability check for a collection of business object data.
     *
     * @param businessObjectDataAvailabilityCollectionRequest the business object data availability collection requests
     *
     * @return the business object data availability information
     */
    BusinessObjectDataAvailabilityCollectionResponse checkBusinessObjectDataAvailabilityCollectionImpl(
        BusinessObjectDataAvailabilityCollectionRequest businessObjectDataAvailabilityCollectionRequest)
    {
        // Perform the validation of the entire request, before we start processing the individual requests that requires the database access.
        validateBusinessObjectDataAvailabilityCollectionRequest(businessObjectDataAvailabilityCollectionRequest);

        // Process the individual requests and build the response.
        BusinessObjectDataAvailabilityCollectionResponse businessObjectDataAvailabilityCollectionResponse =
            new BusinessObjectDataAvailabilityCollectionResponse();
        List<BusinessObjectDataAvailability> businessObjectDataAvailabilityResponses = new ArrayList<>();
        businessObjectDataAvailabilityCollectionResponse.setBusinessObjectDataAvailabilityResponses(businessObjectDataAvailabilityResponses);
        boolean isAllDataAvailable = true;
        boolean isAllDataNotAvailable = true;
        for (BusinessObjectDataAvailabilityRequest request : businessObjectDataAvailabilityCollectionRequest.getBusinessObjectDataAvailabilityRequests())
        {
            // Please note that when calling to process individual availability requests, we ask to skip the request validation and trimming step.
            BusinessObjectDataAvailability businessObjectDataAvailability = checkBusinessObjectDataAvailabilityImpl(request, true);
            businessObjectDataAvailabilityResponses.add(businessObjectDataAvailability);
            isAllDataAvailable = isAllDataAvailable && businessObjectDataAvailability.getNotAvailableStatuses().isEmpty();
            isAllDataNotAvailable = isAllDataNotAvailable && businessObjectDataAvailability.getAvailableStatuses().isEmpty();
        }
        businessObjectDataAvailabilityCollectionResponse.setIsAllDataAvailable(isAllDataAvailable);
        businessObjectDataAvailabilityCollectionResponse.setIsAllDataNotAvailable(isAllDataNotAvailable);

        return businessObjectDataAvailabilityCollectionResponse;
    }

    /**
     * Performs a search and returns a list of business object data key values and relative statuses for a range of requested business object data.
     *
     * @param request the business object data availability request
     *
     * @return the business object data availability information
     */
    BusinessObjectDataAvailability checkBusinessObjectDataAvailabilityImpl(BusinessObjectDataAvailabilityRequest request)
    {
        // By default, validate and trim the request.
        return checkBusinessObjectDataAvailabilityImpl(request, false);
    }

    /**
     * Initiates destruction process for an existing business object data by using S3 tagging to mark the relative S3 files for deletion and updating statuses
     * of the business object data and its storage unit. The S3 data then gets deleted by S3 bucket lifecycle policy that is based on S3 tagging.
     *
     * @param businessObjectDataKey the business object data key
     *
     * @return the business object data information
     */
    BusinessObjectData destroyBusinessObjectDataImpl(BusinessObjectDataKey businessObjectDataKey)
    {
        // Create a business object data destroy parameters DTO.
        BusinessObjectDataDestroyDto businessObjectDataDestroyDto = new BusinessObjectDataDestroyDto();

        // Prepare to initiate a business object data destroy request.
        businessObjectDataInitiateDestroyHelperService.prepareToInitiateDestroy(businessObjectDataDestroyDto, businessObjectDataKey);

        // Create a storage unit notification for the storage unit status change event.
        notificationEventService.processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG,
            businessObjectDataDestroyDto.getBusinessObjectDataKey(), businessObjectDataDestroyDto.getStorageName(),
            businessObjectDataDestroyDto.getNewStorageUnitStatus(), businessObjectDataDestroyDto.getOldStorageUnitStatus());

        // Create a business object data notification for the business object data status change event.
        notificationEventService.processBusinessObjectDataNotificationEventAsync(NotificationEventTypeEntity.EventTypesBdata.BUS_OBJCT_DATA_STTS_CHG,
            businessObjectDataDestroyDto.getBusinessObjectDataKey(), businessObjectDataDestroyDto.getNewBusinessObjectDataStatus(),
            businessObjectDataDestroyDto.getOldBusinessObjectDataStatus());

        // Execute S3 specific steps.
        businessObjectDataInitiateDestroyHelperService.executeS3SpecificSteps(businessObjectDataDestroyDto);

        // Complete the initiation of a business object data destroy request.
        BusinessObjectData businessObjectData = businessObjectDataInitiateDestroyHelperService.executeInitiateDestroyAfterStep(businessObjectDataDestroyDto);

        // Create a storage unit notification for the storage unit status change event.
        notificationEventService.processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG,
            businessObjectDataDestroyDto.getBusinessObjectDataKey(), businessObjectDataDestroyDto.getStorageName(),
            businessObjectDataDestroyDto.getNewStorageUnitStatus(), businessObjectDataDestroyDto.getOldStorageUnitStatus());

        return businessObjectData;
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system to perform queries for a collection of business object data in the specified
     * storages.
     *
     * @param businessObjectDataDdlCollectionRequest the business object data DDL collection request
     *
     * @return the business object data DDL information
     */
    BusinessObjectDataDdlCollectionResponse generateBusinessObjectDataDdlCollectionImpl(
        BusinessObjectDataDdlCollectionRequest businessObjectDataDdlCollectionRequest)
    {
        // Perform the validation of the entire request, before we start processing the individual requests that requires the database access.
        validateBusinessObjectDataDdlCollectionRequest(businessObjectDataDdlCollectionRequest);

        // Process the individual requests and build the response.
        BusinessObjectDataDdlCollectionResponse businessObjectDataDdlCollectionResponse = new BusinessObjectDataDdlCollectionResponse();
        List<BusinessObjectDataDdl> businessObjectDataDdlResponses = new ArrayList<>();
        businessObjectDataDdlCollectionResponse.setBusinessObjectDataDdlResponses(businessObjectDataDdlResponses);
        List<String> ddls = new ArrayList<>();
        for (BusinessObjectDataDdlRequest request : businessObjectDataDdlCollectionRequest.getBusinessObjectDataDdlRequests())
        {
            // Please note that when calling to process individual ddl requests, we ask to skip the request validation and trimming step.
            BusinessObjectDataDdl businessObjectDataDdl = generateBusinessObjectDataDdlImpl(request, true);
            businessObjectDataDdlResponses.add(businessObjectDataDdl);
            ddls.add(businessObjectDataDdl.getDdl());
        }
        businessObjectDataDdlCollectionResponse.setDdlCollection(StringUtils.join(ddls, "\n\n"));

        return businessObjectDataDdlCollectionResponse;
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system to perform queries for a range of requested business object data in the
     * specified storage.
     *
     * @param request the business object data DDL request
     * @param skipRequestValidation specifies whether to skip the request validation and trimming
     *
     * @return the business object data DDL information
     */
    BusinessObjectDataDdl generateBusinessObjectDataDdlImpl(BusinessObjectDataDdlRequest request, boolean skipRequestValidation)
    {
        // Perform the validation.
        if (!skipRequestValidation)
        {
            validateBusinessObjectDataDdlRequest(request);
        }

        return generateDdlOrPartitions(request, false);
    }

    /**
     * Generates the partitions information for a range of requested business object data in the specified storage.
     *
     * @param request the business object data retrieve partitions request
     * @param skipRequestValidation specifies whether to skip the request validation and trimming
     *
     * @return the business object data partitions information
     */
    BusinessObjectDataPartitions generateBusinessObjectDataPartitionsImpl(BusinessObjectDataPartitionsRequest request, boolean skipRequestValidation)
    {
        // Perform the validation.
        if (!skipRequestValidation)
        {
            validateBusinessObjectDataPartitionsRequest(request);
        }

        return generateDdlOrPartitions(businessObjectDataDdlPartitionsHelper.buildBusinessObjectDataDdlRequest(request), true);
    }

    /**
     * @param request business object data DDL request
     * @param isGeneratePartitions flag to indicate if this is a generateDDL or generatePartitions request
     * @param <T>
     *
     * @return business object data DDL object instance if isGenerateDdl = true, otherwise, return business Object Data partitions object instance
     */
    private <T> T generateDdlOrPartitions(BusinessObjectDataDdlRequest request, boolean isGeneratePartitions)
    {
        // Get the business object format entity for the specified parameters and make sure it exists.
        // Please note that when format version is not specified, we should get back the latest format version.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(
            new BusinessObjectFormatKey(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()));

        // Validate that format has schema information.
        Assert.notEmpty(businessObjectFormatEntity.getSchemaColumns(), String.format(
            "Business object format with namespace \"%s\", business object definition name \"%s\", format usage \"%s\", format file type \"%s\"," +
                " and format version \"%s\" doesn't have schema information.",
            businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
            businessObjectFormatEntity.getFileType().getCode(), businessObjectFormatEntity.getBusinessObjectFormatVersion()));

        // If it was specified, retrieve the custom DDL and ensure it exists.
        CustomDdlEntity customDdlEntity = null;
        if (StringUtils.isNotBlank(request.getCustomDdlName()))
        {
            CustomDdlKey customDdlKey = new CustomDdlKey(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
                businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
                businessObjectFormatEntity.getFileType().getCode(), businessObjectFormatEntity.getBusinessObjectFormatVersion(), request.getCustomDdlName());
            customDdlEntity = customDdlDaoHelper.getCustomDdlEntity(customDdlKey);
        }

        // Build a list of storage names specified in the request.
        List<String> storageNames = new ArrayList<>();
        if (StringUtils.isNotBlank(request.getStorageName()))
        {
            storageNames.add(request.getStorageName());
        }
        if (!CollectionUtils.isEmpty(request.getStorageNames()))
        {
            storageNames.addAll(request.getStorageNames());
        }

        // Validate that storage entities, specified in the request, exist, of a proper storage platform type, and have S3 bucket name configured.
        List<StorageEntity> requestedStorageEntities = new ArrayList<>();
        Map<String, StorageEntity> cachedStorageEntities = new HashMap<>();
        Map<String, String> cachedS3BucketNames = new HashMap<>();
        for (String storageName : storageNames)
        {
            // Get the requested storage entity.
            StorageEntity storageEntity = storageDaoHelper.getStorageEntity(storageName);
            requestedStorageEntities.add(storageEntity);

            // Only S3 storage platform is currently supported.
            Assert.isTrue(storageEntity.getStoragePlatform().getName().equals(StoragePlatformEntity.S3),
                String.format("Cannot generate DDL/Partitions for \"%s\" storage platform.", storageEntity.getStoragePlatform().getName()));

            // Validate that storage have S3 bucket name configured. Please note that since S3 bucket name attribute value is required we pass a "true" flag.
            String s3BucketName = storageHelper
                .getStorageAttributeValueByName(configurationHelper.getProperty(ConfigurationValue.S3_ATTRIBUTE_NAME_BUCKET_NAME), storageEntity, true);

            // Memorize retrieved values for faster processing.
            String upperCaseStorageName = storageName.toUpperCase();
            cachedStorageEntities.put(upperCaseStorageName, storageEntity);
            cachedS3BucketNames.put(upperCaseStorageName, s3BucketName);
        }

        if (isGeneratePartitions)
        {
            // Create and initialize a business object data partitions object instance.
            BusinessObjectDataPartitions businessObjectDataPartitions = createBusinessObjectDataPartitions(request);
            businessObjectDataPartitions.setPartitions(businessObjectDataPartitionsHelper
                .generatePartitions(request, businessObjectFormatEntity, storageNames, requestedStorageEntities, cachedStorageEntities, cachedS3BucketNames));

            return (T) businessObjectDataPartitions;
        }
        else
        {
            // Create and initialize a business object data DDL object instance.
            BusinessObjectDataDdl businessObjectDataDdl = createBusinessObjectDataDdl(request);
            businessObjectDataDdl.setDdl(ddlGeneratorFactory.getDdlGenerator(request.getOutputFormat())
                .generateCreateTableDdl(request, businessObjectFormatEntity, customDdlEntity, storageNames, requestedStorageEntities, cachedStorageEntities,
                    cachedS3BucketNames));
            businessObjectDataDdl.setAsOfTime(request.getAsOfTime());

            return (T) businessObjectDataDdl;
        }
    }

    /**
     * Retrieves existing business object data entry information. This method does not start a new transaction and instead continues with existing transaction,
     * if any.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectFormatPartitionKey the business object format partition key, may be null
     * @param businessObjectDataStatus the business object data status, may be null
     * @param includeBusinessObjectDataStatusHistory specifies to include business object data status history in the response
     * @param includeStorageUnitStatusHistory specifies to include storage unit status history for each storage unit in the response
     * @param excludeBusinessObjectDataStorageFiles specifies to exclude storage files in the response
     *
     * @return the retrieved business object data information
     */
    BusinessObjectData getBusinessObjectDataImpl(BusinessObjectDataKey businessObjectDataKey, String businessObjectFormatPartitionKey,
        String businessObjectDataStatus, Boolean includeBusinessObjectDataStatusHistory, Boolean includeStorageUnitStatusHistory,
        Boolean excludeBusinessObjectDataStorageFiles)
    {
        // Validate and trim the business object data key.
        businessObjectDataHelper.validateBusinessObjectDataKey(businessObjectDataKey, false, false);

        // If specified, trim the partition key parameter.
        String businessObjectFormatPartitionKeyLocal = businessObjectFormatPartitionKey != null ? businessObjectFormatPartitionKey.trim() : null;

        // If specified, trim the business object data status parameter; otherwise default to VALID status.
        String businessObjectDataStatusLocal = businessObjectDataStatus != null ? businessObjectDataStatus.trim() : BusinessObjectDataStatusEntity.VALID;

        // Validate the business object data status.
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity =
            businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(businessObjectDataStatusLocal);

        // Get the business object data based on the specified parameters. If a business object data version isn't specified,
        // the latest version of business object data of the specified business object data status is returned.
        BusinessObjectDataEntity businessObjectDataEntity =
            businessObjectDataDaoHelper.getBusinessObjectDataEntityByKeyAndStatus(businessObjectDataKey, businessObjectDataStatusEntity);

        // If specified, ensure the partition key matches what's configured within the business object format.
        if (StringUtils.isNotBlank(businessObjectFormatPartitionKeyLocal))
        {
            String configuredPartitionKey = businessObjectDataEntity.getBusinessObjectFormat().getPartitionKey();
            Assert.isTrue(configuredPartitionKey.equalsIgnoreCase(businessObjectFormatPartitionKeyLocal), String
                .format("Partition key \"%s\" doesn't match configured business object format partition key \"%s\".", businessObjectFormatPartitionKeyLocal,
                    configuredPartitionKey));
        }

        // Create and return the business object definition object from the persisted entity.
        return businessObjectDataHelper
            .createBusinessObjectDataFromEntity(businessObjectDataEntity, includeBusinessObjectDataStatusHistory, includeStorageUnitStatusHistory,
                excludeBusinessObjectDataStorageFiles);
    }

    /**
     * Delegates implementation to {@link org.finra.herd.service.helper.BusinessObjectDataInvalidateUnregisteredHelper}. Keeps current transaction context.
     *
     * @param businessObjectDataInvalidateUnregisteredRequest {@link org.finra.herd.model.api.xml.BusinessObjectDataInvalidateUnregisteredRequest}
     *
     * @return {@link BusinessObjectDataInvalidateUnregisteredResponse}
     */
    BusinessObjectDataInvalidateUnregisteredResponse invalidateUnregisteredBusinessObjectDataImpl(
        BusinessObjectDataInvalidateUnregisteredRequest businessObjectDataInvalidateUnregisteredRequest)
    {
        return businessObjectDataInvalidateUnregisteredHelper.invalidateUnregisteredBusinessObjectData(businessObjectDataInvalidateUnregisteredRequest);
    }

    /**
     * Initiates a restore request for a currently archived business object data. Keeps current transaction context.
     *
     * @param businessObjectDataKey the business object data key
     * @param expirationInDays the the time, in days, between when the business object data is restored to the S3 bucket and when it expires
     * @param archiveRetrievalOption the archive retrieval option when restoring an archived object. Currently three options are supported: Expedited, Standard,
     * and Bulk
     *
     * @return the business object data information
     */
    BusinessObjectData restoreBusinessObjectDataImpl(BusinessObjectDataKey businessObjectDataKey, Integer expirationInDays, String archiveRetrievalOption)
    {
        // Execute the initiate a restore request before step.
        BusinessObjectDataRestoreDto businessObjectDataRestoreDto =
            businessObjectDataInitiateRestoreHelperService.prepareToInitiateRestore(businessObjectDataKey, expirationInDays, archiveRetrievalOption);

        // Create storage unit notification for the origin storage unit.
        notificationEventService.processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG,
            businessObjectDataRestoreDto.getBusinessObjectDataKey(), businessObjectDataRestoreDto.getStorageName(),
            businessObjectDataRestoreDto.getNewStorageUnitStatus(), businessObjectDataRestoreDto.getOldStorageUnitStatus());

        // Initiate the restore request.
        businessObjectDataInitiateRestoreHelperService.executeS3SpecificSteps(businessObjectDataRestoreDto);

        // On failure of the above step, execute the "after" step, and re-throw the exception.
        if (businessObjectDataRestoreDto.getException() != null)
        {
            // On failure, execute the after step that updates the storage unit status back to ARCHIVED.
            businessObjectDataInitiateRestoreHelperService.executeInitiateRestoreAfterStep(businessObjectDataRestoreDto);

            // Create storage unit notification for the origin storage unit.
            notificationEventService.processStorageUnitNotificationEventAsync(NotificationEventTypeEntity.EventTypesStorageUnit.STRGE_UNIT_STTS_CHG,
                businessObjectDataRestoreDto.getBusinessObjectDataKey(), businessObjectDataRestoreDto.getStorageName(),
                businessObjectDataRestoreDto.getNewStorageUnitStatus(), businessObjectDataRestoreDto.getOldStorageUnitStatus());

            // Re-throw the original exception.
            throw businessObjectDataRestoreDto.getException();
        }
        else
        {
            // Execute the after step for the initiate a business object data restore request
            // and return the business object data information.
            return businessObjectDataInitiateRestoreHelperService.executeInitiateRestoreAfterStep(businessObjectDataRestoreDto);
        }
    }

    /**
     * Updates the list of not-available statuses by adding business object data status instances created from discovered "non-available" registered
     * sub-partitions as per list of "matched" partition filters to the specified list of not-available statuses.
     *
     * @param notAvailableStatuses the list of not-available statuses to be updated
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param businessObjectFormatUsage the business object format usage (case-insensitive)
     * @param fileTypeEntity the file type entity
     * @param businessObjectFormatVersion the optional business object format version. If a business object format version isn't specified, the latest available
     * format version for each partition value will be used
     * @param matchedAvailablePartitionFilters the list of "matched" partition filters
     * @param availablePartitions the list of already discovered "available" partitions, where each partition consists of primary and optional sub-partition
     * values
     * @param storageEntities the list of storage entities
     */
    private void addNotAvailableBusinessObjectDataStatuses(List<BusinessObjectDataStatus> notAvailableStatuses,
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity, String businessObjectFormatUsage, FileTypeEntity fileTypeEntity,
        Integer businessObjectFormatVersion, List<List<String>> matchedAvailablePartitionFilters, List<List<String>> availablePartitions,
        List<StorageEntity> storageEntities)
    {
        // Now try to retrieve latest business object data per list of matched filters regardless of business object data and/or storage unit statuses.
        // This is done to include all registered sub-partitions in the response.
        // Business object data availability works across all storage platform types, so the storage platform type is not specified in the herdDao call.
        // We want to select any existing storage units regardless of their status, so we pass "false" for selectOnlyAvailableStorageUnits parameter.
        List<StorageUnitAvailabilityDto> matchedNotAvailableStorageUnitEntities = storageUnitDao
            .getStorageUnitsByPartitionFilters(businessObjectDefinitionEntity, businessObjectFormatUsage, fileTypeEntity, businessObjectFormatVersion,
                matchedAvailablePartitionFilters, null, null, storageEntities, null, null, false, null);

        // Exclude all storage units with business object data having "DELETED" status.
        matchedNotAvailableStorageUnitEntities =
            storageUnitHelper.excludeBusinessObjectDataStatus(matchedNotAvailableStorageUnitEntities, BusinessObjectDataStatusEntity.DELETED);

        // Exclude all already discovered "available" partitions. Please note that, since we got here, the list of matched partitions can not be empty.
        matchedNotAvailableStorageUnitEntities = storageUnitHelper.excludePartitions(matchedNotAvailableStorageUnitEntities, availablePartitions);

        // Keep processing the matched "not available" storage units only when the list is not empty.
        if (!CollectionUtils.isEmpty(matchedNotAvailableStorageUnitEntities))
        {
            // Populate the "not available" statuses with all found "not available" registered sub-partitions.
            addNotAvailableBusinessObjectDataStatuses(notAvailableStatuses, matchedNotAvailableStorageUnitEntities);
        }
    }

    /**
     * Adds business object data status instances created from the list of storage unit availability DTOs to the specified list of not-available statuses.
     *
     * @param notAvailableStatuses the list of not-available statuses
     * @param storageUnitAvailabilityDtos the list of storage unit availability DTOs
     */
    private void addNotAvailableBusinessObjectDataStatuses(List<BusinessObjectDataStatus> notAvailableStatuses,
        List<StorageUnitAvailabilityDto> storageUnitAvailabilityDtos)
    {
        for (StorageUnitAvailabilityDto storageUnitAvailabilityDto : storageUnitAvailabilityDtos)
        {
            notAvailableStatuses.add(createNotAvailableBusinessObjectDataStatus(storageUnitAvailabilityDto));
        }
    }

    /**
     * Performs a search and returns a list of business object data key values and relative statuses for a range of requested business object data.
     *
     * @param request the business object data availability request
     * @param skipRequestValidation specifies whether to skip the request validation and trimming
     *
     * @return the business object data availability information
     */
    private BusinessObjectDataAvailability checkBusinessObjectDataAvailabilityImpl(BusinessObjectDataAvailabilityRequest request, boolean skipRequestValidation)
    {
        // Perform the validation.
        if (!skipRequestValidation)
        {
            validateBusinessObjectDataAvailabilityRequest(request);
        }

        // Get business object format key from the request.
        BusinessObjectFormatKey businessObjectFormatKey = getBusinessObjectFormatKey(request);

        // Make sure that specified business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);

        // Validate that all storage names specified in the request exist and build a list of the requested storage entities.
        List<StorageEntity> storageEntities = new ArrayList<>();
        for (String storageName : getStorageNames(request))
        {
            storageEntities.add(storageDaoHelper.getStorageEntity(storageName));
        }

        // Build partition filters based on the specified partition value filters.
        // Business object data availability works across all storage platform types, so the storage platform type is not specified in the call.
        List<List<String>> partitionFilters = businessObjectDataDaoHelper
            .buildPartitionFilters(request.getPartitionValueFilters(), request.getPartitionValueFilter(), businessObjectFormatKey,
                request.getBusinessObjectDataVersion(), storageEntities, null, null, businessObjectFormatEntity);

        // Get business object data status entity for the VALID status.
        BusinessObjectDataStatusEntity validBusinessObjectDataStatusEntity =
            businessObjectDataStatusDaoHelper.getBusinessObjectDataStatusEntity(BusinessObjectDataStatusEntity.VALID);

        // Retrieve a list of storage unit availability DTOs for the specified partition values. The list will be sorted by partition value that is identified
        // by partition column position. If a business object data version isn't specified, the latest VALID business object data version is returned.
        // Business object data availability works across all storage platform types, so the storage platform type is not specified in the herdDao call.
        // We want to select only "available" storage units, so we pass "true" for selectOnlyAvailableStorageUnits parameter.
        List<StorageUnitAvailabilityDto> availableStorageUnitAvailabilityDtos = storageUnitDao
            .getStorageUnitsByPartitionFilters(businessObjectFormatEntity.getBusinessObjectDefinition(), businessObjectFormatKey.getBusinessObjectFormatUsage(),
                businessObjectFormatEntity.getFileType(), businessObjectFormatKey.getBusinessObjectFormatVersion(), partitionFilters,
                request.getBusinessObjectDataVersion(), validBusinessObjectDataStatusEntity, storageEntities, null, null, true, null);

        // Create business object data availability object instance and initialise it with request field values.
        BusinessObjectDataAvailability businessObjectDataAvailability = createBusinessObjectDataAvailability(request);

        // Create "available" and "not available" business object data status lists.
        List<BusinessObjectDataStatus> availableStatuses = new ArrayList<>();
        businessObjectDataAvailability.setAvailableStatuses(availableStatuses);
        List<BusinessObjectDataStatus> notAvailableStatuses = new ArrayList<>();
        businessObjectDataAvailability.setNotAvailableStatuses(notAvailableStatuses);

        // Build a list of matched available partition filters and populate the available statuses list. Please note that each request partition filter
        // might result in multiple available business object data entities. If storage names are not specified, fail on "duplicate" business object data
        // (same business object data instance registered with multiple storages). Otherwise, remove possible "duplicates".
        List<List<String>> matchedAvailablePartitionFilters = new ArrayList<>();
        List<List<String>> availablePartitions = new ArrayList<>();
        Map<BusinessObjectDataKey, StorageUnitAvailabilityDto> businessObjectDataToStorageUnitMap = new HashMap<>();
        for (StorageUnitAvailabilityDto storageUnitAvailabilityDto : availableStorageUnitAvailabilityDtos)
        {
            BusinessObjectDataKey businessObjectDataKey = storageUnitAvailabilityDto.getBusinessObjectDataKey();

            if (businessObjectDataToStorageUnitMap.containsKey(businessObjectDataKey))
            {
                // If storage is not specified, fail on a business object data registered in multiple storage. Otherwise, ignore that storage unit.
                if (CollectionUtils.isEmpty(storageEntities))
                {
                    throw new IllegalArgumentException(String.format("Found business object data registered in more than one storage. " +
                            "Please specify storage(s) in the request to resolve this. Business object data {%s}",
                        businessObjectDataHelper.businessObjectDataKeyToString(businessObjectDataKey)));
                }
            }
            else
            {
                matchedAvailablePartitionFilters.add(businessObjectDataHelper.getPartitionFilter(businessObjectDataKey, partitionFilters.get(0)));
                availablePartitions.add(businessObjectDataHelper.getPrimaryAndSubPartitionValues(businessObjectDataKey));
                availableStatuses.add(createAvailableBusinessObjectDataStatus(storageUnitAvailabilityDto));
                businessObjectDataToStorageUnitMap.put(businessObjectDataKey, storageUnitAvailabilityDto);
            }
        }

        // Check if request specifies to include all registered sub-partitions in the response.
        boolean includeAllRegisteredSubPartitions =
            request.getBusinessObjectDataVersion() == null && BooleanUtils.isTrue(request.isIncludeAllRegisteredSubPartitions());

        // If request specifies to include all registered sub-partitions in the response, query all
        // matched partition filters one more time to discover any non-available registered sub-partitions.
        if (includeAllRegisteredSubPartitions && !CollectionUtils.isEmpty(matchedAvailablePartitionFilters))
        {
            addNotAvailableBusinessObjectDataStatuses(notAvailableStatuses, businessObjectFormatEntity.getBusinessObjectDefinition(),
                businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatEntity.getFileType(),
                businessObjectFormatKey.getBusinessObjectFormatVersion(), matchedAvailablePartitionFilters, availablePartitions, storageEntities);
        }

        // Get a list of unmatched partition filters.
        List<List<String>> unmatchedPartitionFilters = new ArrayList<>(partitionFilters);
        unmatchedPartitionFilters.removeAll(matchedAvailablePartitionFilters);

        // We still need to try to retrieve business object data per list of unmatched filters regardless of business object data and/or storage unit statuses.
        // This is done to populate not-available statuses with legitimate reasons.
        // Business object data availability works across all storage platform types, so the storage platform type is not specified in the herdDao call.
        // We want to select any existing storage units regardless of their status, so we pass "false" for selectOnlyAvailableStorageUnits parameter.
        List<StorageUnitAvailabilityDto> notAvailableStorageUnitAvailabilityDtos = storageUnitDao
            .getStorageUnitsByPartitionFilters(businessObjectFormatEntity.getBusinessObjectDefinition(), businessObjectFormatKey.getBusinessObjectFormatUsage(),
                businessObjectFormatEntity.getFileType(), businessObjectFormatKey.getBusinessObjectFormatVersion(), unmatchedPartitionFilters,
                request.getBusinessObjectDataVersion(), null, storageEntities, null, null, false, null);

        // Populate the not-available statuses list.
        addNotAvailableBusinessObjectDataStatuses(notAvailableStatuses, notAvailableStorageUnitAvailabilityDtos);

        // Build a list of matched "not-available" partition filters.
        // Please note that each request partition filter might result in multiple available business object data entities.
        List<List<String>> matchedNotAvailablePartitionFilters = getPartitionFilters(notAvailableStorageUnitAvailabilityDtos, partitionFilters.get(0));

        // Update the list of unmatched partition filters.
        unmatchedPartitionFilters.removeAll(matchedNotAvailablePartitionFilters);

        // Populate the "not available" statuses per remaining unmatched filters.
        for (List<String> unmatchedPartitionFilter : unmatchedPartitionFilters)
        {
            notAvailableStatuses.add(createNotAvailableBusinessObjectDataStatus(request, unmatchedPartitionFilter, REASON_NOT_REGISTERED));
        }

        return businessObjectDataAvailability;
    }

    /**
     * Creates a business object data status instance from the storage unit availability DTO.
     *
     * @param storageUnitAvailabilityDto the storage unit availability DTO
     *
     * @return the business object data status instance
     */
    private BusinessObjectDataStatus createAvailableBusinessObjectDataStatus(StorageUnitAvailabilityDto storageUnitAvailabilityDto)
    {
        BusinessObjectDataStatus businessObjectDataStatus = new BusinessObjectDataStatus();

        businessObjectDataStatus.setBusinessObjectFormatVersion(storageUnitAvailabilityDto.getBusinessObjectDataKey().getBusinessObjectFormatVersion());
        businessObjectDataStatus.setPartitionValue(storageUnitAvailabilityDto.getBusinessObjectDataKey().getPartitionValue());
        businessObjectDataStatus.setSubPartitionValues(storageUnitAvailabilityDto.getBusinessObjectDataKey().getSubPartitionValues());
        businessObjectDataStatus.setBusinessObjectDataVersion(storageUnitAvailabilityDto.getBusinessObjectDataKey().getBusinessObjectDataVersion());
        businessObjectDataStatus.setReason(storageUnitAvailabilityDto.getBusinessObjectDataStatus());

        return businessObjectDataStatus;
    }

    /**
     * Creates business object data availability object instance and initialise it with the business object data availability request field values.
     *
     * @param request the business object data availability request
     *
     * @return the newly created BusinessObjectDataAvailability object instance
     */
    private BusinessObjectDataAvailability createBusinessObjectDataAvailability(BusinessObjectDataAvailabilityRequest request)
    {
        BusinessObjectDataAvailability businessObjectDataAvailability = new BusinessObjectDataAvailability();

        businessObjectDataAvailability.setNamespace(request.getNamespace());
        businessObjectDataAvailability.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName());
        businessObjectDataAvailability.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage());
        businessObjectDataAvailability.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType());
        businessObjectDataAvailability.setBusinessObjectFormatVersion(request.getBusinessObjectFormatVersion());

        businessObjectDataAvailability.setPartitionValueFilters(request.getPartitionValueFilters());
        businessObjectDataAvailability.setPartitionValueFilter(request.getPartitionValueFilter());

        businessObjectDataAvailability.setBusinessObjectDataVersion(request.getBusinessObjectDataVersion());

        businessObjectDataAvailability.setStorageNames(request.getStorageNames());
        businessObjectDataAvailability.setStorageName(request.getStorageName());

        return businessObjectDataAvailability;
    }

    /**
     * Creates business object data ddl object instance and initialise it with the business object data ddl request field values.
     *
     * @param request the business object data ddl request
     *
     * @return the newly created BusinessObjectDataDdl object instance
     */
    private BusinessObjectDataDdl createBusinessObjectDataDdl(BusinessObjectDataDdlRequest request)
    {
        BusinessObjectDataDdl businessObjectDataDdl = new BusinessObjectDataDdl();

        businessObjectDataDdl.setNamespace(request.getNamespace());
        businessObjectDataDdl.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName());
        businessObjectDataDdl.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage());
        businessObjectDataDdl.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType());
        businessObjectDataDdl.setBusinessObjectFormatVersion(request.getBusinessObjectFormatVersion());

        businessObjectDataDdl.setPartitionValueFilters(request.getPartitionValueFilters());
        businessObjectDataDdl.setPartitionValueFilter(request.getPartitionValueFilter());

        businessObjectDataDdl.setBusinessObjectDataVersion(request.getBusinessObjectDataVersion());

        businessObjectDataDdl.setStorageNames(request.getStorageNames());
        businessObjectDataDdl.setStorageName(request.getStorageName());

        businessObjectDataDdl.setOutputFormat(request.getOutputFormat());
        businessObjectDataDdl.setTableName(request.getTableName());
        businessObjectDataDdl.setCustomDdlName(request.getCustomDdlName());

        return businessObjectDataDdl;
    }

    /**
     * Creates business object data partitions object instance and initialise it with the business object data partitions request field values.
     *
     * @param request the business object data ddl request
     *
     * @return the newly created BusinessObjectDataPartitions object instance
     */
    private BusinessObjectDataPartitions createBusinessObjectDataPartitions(BusinessObjectDataDdlRequest request)
    {
        BusinessObjectDataPartitions businessObjectDataPartitions = new BusinessObjectDataPartitions();

        businessObjectDataPartitions.setNamespace(request.getNamespace());
        businessObjectDataPartitions.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName());
        businessObjectDataPartitions.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage());
        businessObjectDataPartitions.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType());
        businessObjectDataPartitions.setBusinessObjectFormatVersion(request.getBusinessObjectFormatVersion());

        businessObjectDataPartitions.setPartitionValueFilters(request.getPartitionValueFilters());

        businessObjectDataPartitions.setBusinessObjectDataVersion(request.getBusinessObjectDataVersion());

        businessObjectDataPartitions.setStorageNames(request.getStorageNames());

        return businessObjectDataPartitions;
    }

    /**
     * Creates a business object data status instance from the storage unit availability DTO.
     *
     * @param storageUnitAvailabilityDto the storage unit availability DTO
     *
     * @return the business object data status instance
     */
    private BusinessObjectDataStatus createNotAvailableBusinessObjectDataStatus(StorageUnitAvailabilityDto storageUnitAvailabilityDto)
    {
        BusinessObjectDataStatus businessObjectDataStatus = new BusinessObjectDataStatus();

        businessObjectDataStatus.setBusinessObjectFormatVersion(storageUnitAvailabilityDto.getBusinessObjectDataKey().getBusinessObjectFormatVersion());
        businessObjectDataStatus.setPartitionValue(storageUnitAvailabilityDto.getBusinessObjectDataKey().getPartitionValue());
        businessObjectDataStatus.setSubPartitionValues(storageUnitAvailabilityDto.getBusinessObjectDataKey().getSubPartitionValues());
        businessObjectDataStatus.setBusinessObjectDataVersion(storageUnitAvailabilityDto.getBusinessObjectDataKey().getBusinessObjectDataVersion());

        // If storage unit is "available", the business object data is selected as "non-available" due to its business object data status.
        if (storageUnitAvailabilityDto.isStorageUnitAvailable())
        {
            businessObjectDataStatus.setReason(storageUnitAvailabilityDto.getBusinessObjectDataStatus());
        }
        // Otherwise, report the storage unit status as a reason for the business object data not being available.
        else
        {
            businessObjectDataStatus.setReason(storageUnitAvailabilityDto.getStorageUnitStatus());
        }

        return businessObjectDataStatus;
    }

    /**
     * Creates the business object data status.
     *
     * @param businessObjectDataAvailabilityRequest the business object data availability request
     * @param unmatchedPartitionFilter the partition filter that got no matched business object data instances
     * @param reason the reason for the business object data not being available
     *
     * @return the business object data status
     */
    private BusinessObjectDataStatus createNotAvailableBusinessObjectDataStatus(BusinessObjectDataAvailabilityRequest businessObjectDataAvailabilityRequest,
        List<String> unmatchedPartitionFilter, String reason)
    {
        BusinessObjectDataStatus businessObjectDataStatus = new BusinessObjectDataStatus();

        // Populate business object data status values using the business object data availability request.
        businessObjectDataStatus.setBusinessObjectFormatVersion(businessObjectDataAvailabilityRequest.getBusinessObjectFormatVersion());

        // When list of partition value filters is used, we populate primary and/or sub-partition values.
        if (businessObjectDataAvailabilityRequest.getPartitionValueFilters() != null)
        {
            // Replace all null partition values with an empty string.
            replaceAllNullsWithEmptyString(unmatchedPartitionFilter);

            // Populate primary and sub-partition values from the unmatched partition filter.
            businessObjectDataStatus.setPartitionValue(unmatchedPartitionFilter.get(0));
            businessObjectDataStatus.setSubPartitionValues(unmatchedPartitionFilter.subList(1, unmatchedPartitionFilter.size()));
        }
        // Otherwise, for backwards compatibility, populate primary partition value only per expected single partition value from the unmatched filter.
        else
        {
            // Since the availability request contains a standalone partition value filter,
            // the unmatched partition filter is expected to contain only a single partition value.
            for (String partitionValue : unmatchedPartitionFilter)
            {
                if (partitionValue != null)
                {
                    businessObjectDataStatus.setPartitionValue(partitionValue);
                    break;
                }
            }
        }
        businessObjectDataStatus.setBusinessObjectDataVersion(businessObjectDataAvailabilityRequest.getBusinessObjectDataVersion());
        businessObjectDataStatus.setReason(reason);

        return businessObjectDataStatus;
    }

    /**
     * Returns all parts of business object data alternate key without business object format and business object data versions as a list of strings. Please
     * note that business object format usage is returned in all uppercase, since it is not a database lookup value.
     *
     * @param businessObjectData the business object data
     *
     * @return the list of business object data alternate key values without business object format and business object data versions
     */
    private List<String> getBusinessObjectDataVersionLessAlternateKeyValues(BusinessObjectData businessObjectData)
    {
        List<String> alternateKeyValues = new ArrayList<>();

        alternateKeyValues.add(businessObjectData.getNamespace());
        alternateKeyValues.add(businessObjectData.getBusinessObjectDefinitionName());
        alternateKeyValues.add(businessObjectData.getBusinessObjectFormatUsage().toUpperCase());
        alternateKeyValues.add(businessObjectData.getBusinessObjectFormatFileType());
        alternateKeyValues.add(businessObjectData.getPartitionValue());

        if (CollectionUtils.isNotEmpty(businessObjectData.getSubPartitionValues()))
        {
            alternateKeyValues.addAll(businessObjectData.getSubPartitionValues());
        }

        return alternateKeyValues;
    }

    /**
     * Gets business object format key from the business object data availability request.
     *
     * @param request the business object data availability request
     *
     * @return the business object format key
     */
    private BusinessObjectFormatKey getBusinessObjectFormatKey(BusinessObjectDataAvailabilityRequest request)
    {
        return new BusinessObjectFormatKey(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
            request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion());
    }

    /**
     * Gets a list of matched partition filters per specified list of storage unit entities and a sample partition filter.
     *
     * @param storageUnitAvailabilityDtos the list of storage unit availability DTOs
     * @param samplePartitionFilter the sample partition filter
     *
     * @return the list of partition filters
     */
    private List<List<String>> getPartitionFilters(List<StorageUnitAvailabilityDto> storageUnitAvailabilityDtos, List<String> samplePartitionFilter)
    {
        List<List<String>> partitionFilters = new ArrayList<>();

        for (StorageUnitAvailabilityDto storageUnitAvailabilityDto : storageUnitAvailabilityDtos)
        {
            partitionFilters.add(businessObjectDataHelper.getPartitionFilter(storageUnitAvailabilityDto.getBusinessObjectDataKey(), samplePartitionFilter));
        }

        return partitionFilters;
    }

    /**
     * Gets storage names from the business object data availability request.
     *
     * @param request the business object data availability request
     *
     * @return the list of storage names
     */
    private List<String> getStorageNames(BusinessObjectDataAvailabilityRequest request)
    {
        List<String> storageNames = new ArrayList<>();

        if (StringUtils.isNotBlank(request.getStorageName()))
        {
            storageNames.add(request.getStorageName());
        }

        if (!CollectionUtils.isEmpty(request.getStorageNames()))
        {
            storageNames.addAll(request.getStorageNames());
        }

        return storageNames;
    }

    /**
     * Replaces all null values in the specified list with empty strings.
     *
     * @param list the list of strings
     */
    private void replaceAllNullsWithEmptyString(List<String> list)
    {
        for (int i = 0; i < list.size(); i++)
        {
            if (list.get(i) == null)
            {
                list.set(i, "");
            }
        }
    }

    /**
     * Validates a business object data availability collection request. This method also trims appropriate request parameters.
     *
     * @param businessObjectDataAvailabilityCollectionRequest the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectDataAvailabilityCollectionRequest(
        BusinessObjectDataAvailabilityCollectionRequest businessObjectDataAvailabilityCollectionRequest)
    {
        Assert.notNull(businessObjectDataAvailabilityCollectionRequest, "A business object data availability collection request must be specified.");

        Assert.isTrue(!CollectionUtils.isEmpty(businessObjectDataAvailabilityCollectionRequest.getBusinessObjectDataAvailabilityRequests()),
            "At least one business object data availability request must be specified.");

        for (BusinessObjectDataAvailabilityRequest request : businessObjectDataAvailabilityCollectionRequest.getBusinessObjectDataAvailabilityRequests())
        {
            validateBusinessObjectDataAvailabilityRequest(request);
        }
    }

    /**
     * Validates the business object data availability request. This method also trims appropriate request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectDataAvailabilityRequest(BusinessObjectDataAvailabilityRequest request)
    {
        Assert.notNull(request, "A business object data availability request must be specified.");

        // Validate and trim the request parameters.
        Assert.hasText(request.getNamespace(), "A namespace must be specified.");
        request.setNamespace(request.getNamespace().trim());

        Assert.hasText(request.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().trim());

        Assert.hasText(request.getBusinessObjectFormatUsage(), "A business object format usage must be specified.");
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().trim());

        Assert.hasText(request.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().trim());

        // Validate the partition value filters. Allow partition value tokens to be specified.
        businessObjectDataHelper.validatePartitionValueFilters(request.getPartitionValueFilters(), request.getPartitionValueFilter(), true);

        // Make sure that request does not contain both a list of storage names and a standalone storage name.
        Assert.isTrue(request.getStorageNames() == null || request.getStorageName() == null,
            "A list of storage names and a standalone storage name cannot be both specified.");

        // Trim the standalone storage name, if specified.
        if (request.getStorageName() != null)
        {
            Assert.hasText(request.getStorageName(), "A storage name must be specified.");
            request.setStorageName(request.getStorageName().trim());
        }

        // Validate and trim the list of storage names.
        if (!CollectionUtils.isEmpty(request.getStorageNames()))
        {
            for (int i = 0; i < request.getStorageNames().size(); i++)
            {
                Assert.hasText(request.getStorageNames().get(i), "A storage name must be specified.");
                request.getStorageNames().set(i, request.getStorageNames().get(i).trim());
            }
        }
    }

    /**
     * Validates a business object data DDL collection request. This method also trims appropriate request parameters.
     *
     * @param businessObjectDataDdlCollectionRequest the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectDataDdlCollectionRequest(BusinessObjectDataDdlCollectionRequest businessObjectDataDdlCollectionRequest)
    {
        Assert.notNull(businessObjectDataDdlCollectionRequest, "A business object data DDL collection request must be specified.");

        Assert.isTrue(!CollectionUtils.isEmpty(businessObjectDataDdlCollectionRequest.getBusinessObjectDataDdlRequests()),
            "At least one business object data DDL request must be specified.");

        for (BusinessObjectDataDdlRequest request : businessObjectDataDdlCollectionRequest.getBusinessObjectDataDdlRequests())
        {
            validateBusinessObjectDataDdlRequest(request);
        }
    }

    /**
     * Validates the business object data DDL request. This method also trims appropriate request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectDataDdlRequest(BusinessObjectDataDdlRequest request)
    {
        Assert.notNull(request, "A business object data DDL request must be specified.");

        // Validate and trim the request parameters.
        Assert.hasText(request.getNamespace(), "A namespace must be specified.");
        request.setNamespace(request.getNamespace().trim());

        Assert.hasText(request.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().trim());

        Assert.hasText(request.getBusinessObjectFormatUsage(), "A business object format usage must be specified.");
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().trim());

        Assert.hasText(request.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().trim());

        // Validate the partition value filters. Do not allow partition value tokens to be specified.
        businessObjectDataHelper.validatePartitionValueFilters(request.getPartitionValueFilters(), request.getPartitionValueFilter(), false);

        // Make sure that request does not contain both a list of storage names and a standalone storage name.
        Assert.isTrue(request.getStorageNames() == null || request.getStorageName() == null,
            "A list of storage names and a standalone storage name cannot be both specified.");

        // Trim the standalone storage name, if specified.
        if (request.getStorageName() != null)
        {
            Assert.hasText(request.getStorageName(), "A storage name must be specified.");
            request.setStorageName(request.getStorageName().trim());
        }

        // Validate and trim the list of storage names.
        if (!CollectionUtils.isEmpty(request.getStorageNames()))
        {
            for (int i = 0; i < request.getStorageNames().size(); i++)
            {
                Assert.hasText(request.getStorageNames().get(i), "A storage name must be specified.");
                request.getStorageNames().set(i, request.getStorageNames().get(i).trim());
            }
        }

        Assert.notNull(request.getOutputFormat(), "An output format must be specified.");

        Assert.hasText(request.getTableName(), "A table name must be specified.");
        request.setTableName(request.getTableName().trim());

        if (StringUtils.isNotBlank(request.getCustomDdlName()))
        {
            request.setCustomDdlName(request.getCustomDdlName().trim());
        }
    }

    /**
     * Validates the business object data partitions request. This method also trims appropriate request parameters.
     *
     * @param request the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectDataPartitionsRequest(BusinessObjectDataPartitionsRequest request)
    {
        Assert.notNull(request, "A business object data partitions request must be specified.");

        // Validate and trim the request parameters.
        Assert.hasText(request.getNamespace(), "A namespace must be specified.");
        request.setNamespace(request.getNamespace().trim());

        Assert.hasText(request.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().trim());

        Assert.hasText(request.getBusinessObjectFormatUsage(), "A business object format usage must be specified.");
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().trim());

        Assert.hasText(request.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().trim());

        // Validate the partition value filters. Do not allow partition value tokens to be specified.
        businessObjectDataHelper.validatePartitionValueFilters(request.getPartitionValueFilters(), null, false);

        // Validate and trim the list of storage names.
        if (CollectionUtils.isNotEmpty(request.getStorageNames()))
        {
            for (int i = 0; i < request.getStorageNames().size(); i++)
            {
                Assert.hasText(request.getStorageNames().get(i), "A storage name must be specified.");
                request.getStorageNames().set(i, request.getStorageNames().get(i).trim());
            }
        }
    }
}
