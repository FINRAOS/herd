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

import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_CREATE;
import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_DELETE;
import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_UPDATE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.HerdStringUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.IndexFunctionsDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.dao.helper.TagDaoHelper;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionChangeEvent;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptiveInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKeys;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.DescriptiveBusinessObjectFormat;
import org.finra.herd.model.api.xml.DescriptiveBusinessObjectFormatUpdateRequest;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.SampleDataFile;
import org.finra.herd.model.dto.BusinessObjectDefinitionSampleFileUpdateDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.SearchIndexUpdateDto;
import org.finra.herd.model.jpa.BusinessObjectDefinitionAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionSampleDataFileEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.StorageEntity;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.service.BusinessObjectDefinitionService;
import org.finra.herd.service.FacetFieldValidationService;
import org.finra.herd.service.SearchableService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.AttributeHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.SearchIndexUpdateHelper;
import org.finra.herd.service.helper.StorageDaoHelper;
import org.finra.herd.service.helper.TagHelper;

/**
 * The business object definition service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
// TODO: Refactor the service implementation and remove the PMD suppress warning statement below.
@SuppressWarnings("PMD.TooManyMethods")
public class BusinessObjectDefinitionServiceImpl implements BusinessObjectDefinitionService, SearchableService, FacetFieldValidationService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessObjectDefinitionServiceImpl.class);

    /**
     * The size of the chunks to use when updating search index documents based on a list of ids.
     */
    public static final int UPDATE_SEARCH_INDEX_DOCUMENT_CHUNK_SIZE = 100;

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private AttributeHelper attributeHelper;

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Autowired
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private IndexFunctionsDao indexFunctionsDao;

    @Autowired
    private TagHelper tagHelper;

    @Autowired
    private TagDaoHelper tagDaoHelper;

    @Autowired
    private StorageDaoHelper storageDaoHelper;

    @Autowired
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

    // Constant to hold the data provider name option for the business object definition search
    private static final String DATA_PROVIDER_NAME_FIELD = "dataprovidername";

    // Constant to hold the short description option for the business object definition search
    private static final String SHORT_DESCRIPTION_FIELD = "shortdescription";

    // Constant to hold the display name option for the business object definition search
    private static final String DISPLAY_NAME_FIELD = "displayname";

    private static final String TAG_FACET_FIELD = "tag";

    /**
     * {@inheritDoc}
     * <p>
     * This implementation starts a new transaction.
     */
    @NamespacePermission(fields = "#request.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDefinition createBusinessObjectDefinition(BusinessObjectDefinitionCreateRequest request)
    {
        return createBusinessObjectDefinitionImpl(request);
    }

    /**
     * Creates a new business object definition.
     *
     * @param request the business object definition create request
     *
     * @return the created business object definition
     */
    protected BusinessObjectDefinition createBusinessObjectDefinitionImpl(BusinessObjectDefinitionCreateRequest request)
    {
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper.createBusinessObjectDefinitionEntity(request);

        // Notify the search index that a business object definition must be created.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_CREATE);

        // Create and return the business object definition object from the persisted entity.
        return createBusinessObjectDefinitionFromEntity(businessObjectDefinitionEntity, false);
    }

    @Override
    public boolean indexSizeCheckValidationBusinessObjectDefinitions(String indexName)
    {
        final String documentType = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);

        // Simple count validation, index size should equal entity list size
        final long indexSize = indexFunctionsDao.getNumberOfTypesInIndex(indexName, documentType);
        final long businessObjectDefinitionDatabaseTableSize = businessObjectDefinitionDao.getCountOfAllBusinessObjectDefinitions();
        if (businessObjectDefinitionDatabaseTableSize != indexSize)
        {
            LOGGER.error("Index validation failed, business object definition database table size {}, does not equal index size {}.",
                businessObjectDefinitionDatabaseTableSize, indexSize);
        }

        return businessObjectDefinitionDatabaseTableSize == indexSize;
    }

    @Override
    public boolean indexSpotCheckPercentageValidationBusinessObjectDefinitions(String indexName)
    {
        final Double spotCheckPercentage = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_PERCENTAGE, Double.class);

        // Get a list of all business object definitions
        final List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList =
            Collections.unmodifiableList(businessObjectDefinitionDao.getPercentageOfAllBusinessObjectDefinitions(spotCheckPercentage));

        return indexValidateBusinessObjectDefinitionsList(businessObjectDefinitionEntityList, indexName);
    }

    @Override
    public boolean indexSpotCheckMostRecentValidationBusinessObjectDefinitions(String indexName)
    {
        final Integer spotCheckMostRecentNumber =
            configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);

        // Get a list of all business object definitions
        final List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList =
            Collections.unmodifiableList(businessObjectDefinitionDao.getMostRecentBusinessObjectDefinitions(spotCheckMostRecentNumber));

        return indexValidateBusinessObjectDefinitionsList(businessObjectDefinitionEntityList, indexName);
    }

    @Override
    @Async
    public Future<Void> indexValidateAllBusinessObjectDefinitions(String indexName)
    {
        final String documentType = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);

        // Get a list of all business object definitions
        final List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList =
            Collections.unmodifiableList(businessObjectDefinitionDao.getAllBusinessObjectDefinitions());

        // Remove any index documents that are not in the database
        removeAnyIndexDocumentsThatAreNotInBusinessObjectsDefinitionsList(indexName, documentType, businessObjectDefinitionEntityList);

        // Validate all Business Object Definitions
        businessObjectDefinitionHelper.executeFunctionForBusinessObjectDefinitionEntities(indexName, documentType, businessObjectDefinitionEntityList,
            indexFunctionsDao::validateDocumentIndex);

        // Return an AsyncResult so callers will know the future is "done". They can call "isDone" to know when this method has completed and they
        // can call "get" to see if any exceptions were thrown.
        return new AsyncResult<>(null);
    }

    /**
     * Method to remove business object definitions in the index that don't exist in the database
     *
     * @param indexName the name of the index
     * @param documentType the document type
     * @param businessObjectDefinitionEntityList list of business object definitions in the database
     */
    private void removeAnyIndexDocumentsThatAreNotInBusinessObjectsDefinitionsList(final String indexName, final String documentType,
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList)
    {
        // Get a list of business object definition ids from the list of business object definition entities in the database
        List<String> databaseBusinessObjectDefinitionIdList = new ArrayList<>();
        businessObjectDefinitionEntityList
            .forEach(businessObjectDefinitionEntity -> databaseBusinessObjectDefinitionIdList.add(businessObjectDefinitionEntity.getId().toString()));

        // Get a list of business object definition ids in the search index
        List<String> indexDocumentBusinessObjectDefinitionIdList = indexFunctionsDao.getIdsInIndex(indexName, documentType);

        // Remove the database ids from the index ids
        indexDocumentBusinessObjectDefinitionIdList.removeAll(databaseBusinessObjectDefinitionIdList);

        // If there are any ids left in the index list they need to be removed
        indexDocumentBusinessObjectDefinitionIdList.forEach(id -> indexFunctionsDao.deleteDocumentById(indexName, documentType, id));
    }

    /**
     * A helper method that will validate a list of business object definitions
     *
     * @param businessObjectDefinitionEntityList the list of business object definitions that will be validated
     *
     * @return true all of the business object definitions are valid in the index
     */
    private boolean indexValidateBusinessObjectDefinitionsList(final List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList, String indexName)
    {
        final String documentType = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);

        Predicate<BusinessObjectDefinitionEntity> validInIndexPredicate = businessObjectDefinitionEntity ->
        {
            // Fetch Join with .size()
            businessObjectDefinitionEntity.getAttributes().size();
            businessObjectDefinitionEntity.getBusinessObjectDefinitionTags().size();
            businessObjectDefinitionEntity.getBusinessObjectFormats().size();
            businessObjectDefinitionEntity.getColumns().size();
            businessObjectDefinitionEntity.getSampleDataFiles().size();

            // Convert the business object definition entity to a JSON string
            final String jsonString = businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(businessObjectDefinitionEntity);

            return indexFunctionsDao.isValidDocumentIndex(indexName, documentType, businessObjectDefinitionEntity.getId().toString(), jsonString);
        };

        boolean isValid = true;
        for (BusinessObjectDefinitionEntity businessObjectDefinitionEntity : businessObjectDefinitionEntityList)
        {
            if (!validInIndexPredicate.test(businessObjectDefinitionEntity))
            {
                isValid = false;
            }
        }

        return isValid;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation starts a new transaction.
     */
    @NamespacePermission(fields = "#businessObjectDefinitionKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDefinition updateBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        BusinessObjectDefinitionUpdateRequest request)
    {
        return updateBusinessObjectDefinitionImpl(businessObjectDefinitionKey, request);
    }

    /**
     * Updates a business object definition.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param request the business object definition update request
     *
     * @return the updated business object definition
     */
    protected BusinessObjectDefinition updateBusinessObjectDefinitionImpl(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        BusinessObjectDefinitionUpdateRequest request)
    {
        // Perform validation and trim.
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);
        validateBusinessObjectDefinitionUpdateRequest(request);

        // Retrieve and ensure that a business object definition already exists with the specified key.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Update and persist the entity.
        updateBusinessObjectDefinitionEntity(businessObjectDefinitionEntity, request);

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Create and return the business object definition object from the persisted entity.
        return createBusinessObjectDefinitionFromEntity(businessObjectDefinitionEntity, false);
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation starts a new transaction.
     */
    @NamespacePermission(fields = "#businessObjectDefinitionKey.namespace", permissions = {NamespacePermissionEnum.WRITE_DESCRIPTIVE_CONTENT,
        NamespacePermissionEnum.WRITE})
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDefinition updateBusinessObjectDefinitionDescriptiveInformation(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        BusinessObjectDefinitionDescriptiveInformationUpdateRequest request)
    {
        return updateBusinessObjectDefinitionDescriptiveInformationImpl(businessObjectDefinitionKey, request);
    }

    /**
     * Updates a business object definition descriptive information.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param request the business object definition descriptive information update request
     *
     * @return the updated business object definition
     */
    protected BusinessObjectDefinition updateBusinessObjectDefinitionDescriptiveInformationImpl(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        BusinessObjectDefinitionDescriptiveInformationUpdateRequest request)
    {
        // Perform validation and trim.
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);
        validateBusinessObjectDefinitionDescriptiveInformationUpdateRequest(request);

        // Retrieve and ensure that a business object definition already exists with the specified key.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        BusinessObjectFormatEntity businessObjectFormatEntity = null;
        DescriptiveBusinessObjectFormatUpdateRequest descriptiveFormat = request.getDescriptiveBusinessObjectFormat();
        if (descriptiveFormat != null)
        {
            BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey();
            businessObjectFormatKey.setBusinessObjectDefinitionName(businessObjectDefinitionEntity.getName());
            businessObjectFormatKey.setNamespace(businessObjectDefinitionEntity.getNamespace().getCode());
            businessObjectFormatKey.setBusinessObjectFormatFileType(descriptiveFormat.getBusinessObjectFormatFileType());
            businessObjectFormatKey.setBusinessObjectFormatUsage(descriptiveFormat.getBusinessObjectFormatUsage());
            businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);
        }
        businessObjectDefinitionEntity.setDescriptiveBusinessObjectFormat(businessObjectFormatEntity);

        // Update and persist the entity.
        updateBusinessObjectDefinitionEntityDescriptiveInformation(businessObjectDefinitionEntity, request);

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Create and return the business object definition object from the persisted entity.
        return createBusinessObjectDefinitionFromEntity(businessObjectDefinitionEntity, false);
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDefinition getBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        Boolean includeBusinessObjectDefinitionUpdateHistory)
    {
        return getBusinessObjectDefinitionImpl(businessObjectDefinitionKey, includeBusinessObjectDefinitionUpdateHistory);
    }

    /**
     * Gets a business object definition for the specified key.
     *
     * @param businessObjectDefinitionKey the business object definition key
     * @param includeBusinessObjectDefinitionUpdateHistory a flag to include change event information or not
     *
     * @return the business object definition.
     */
    protected BusinessObjectDefinition getBusinessObjectDefinitionImpl(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        Boolean includeBusinessObjectDefinitionUpdateHistory)
    {
        // Perform validation and trim.
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);

        // Retrieve and ensure that a business object definition already exists with the specified key.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Create and return the business object definition object from the persisted entity.
        return createBusinessObjectDefinitionFromEntity(businessObjectDefinitionEntity, includeBusinessObjectDefinitionUpdateHistory);
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation starts a new transaction.
     */
    @NamespacePermission(fields = "#businessObjectDefinitionKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectDefinition deleteBusinessObjectDefinition(BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        return deleteBusinessObjectDefinitionImpl(businessObjectDefinitionKey);
    }

    /**
     * Deletes a business object definition for the specified name.
     *
     * @param businessObjectDefinitionKey the business object definition key
     *
     * @return the business object definition that was deleted.
     */
    protected BusinessObjectDefinition deleteBusinessObjectDefinitionImpl(BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        // Perform validation and trim.
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);

        // Retrieve and ensure that a business object definition already exists with the specified key.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Delete the business object definition.
        businessObjectDefinitionDao.delete(businessObjectDefinitionEntity);

        // Notify the search index that a business object definition must be deleted.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_DELETE);

        // Create and return the business object definition object from the deleted entity.
        return createBusinessObjectDefinitionFromEntity(businessObjectDefinitionEntity, false);
    }

    @Override
    public BusinessObjectDefinitionKeys getBusinessObjectDefinitions()
    {
        BusinessObjectDefinitionKeys businessObjectDefinitionKeys = new BusinessObjectDefinitionKeys();
        businessObjectDefinitionKeys.getBusinessObjectDefinitionKeys().addAll(businessObjectDefinitionDao.getBusinessObjectDefinitionKeys());
        return businessObjectDefinitionKeys;
    }

    @Override
    public BusinessObjectDefinitionKeys getBusinessObjectDefinitions(String namespaceCode)
    {
        // Validate and trim the namespace code.
        Assert.hasText(namespaceCode, "A namespace must be specified.");

        // Retrieve and return the list of business object definitions
        BusinessObjectDefinitionKeys businessObjectDefinitionKeys = new BusinessObjectDefinitionKeys();
        businessObjectDefinitionKeys.getBusinessObjectDefinitionKeys()
            .addAll(businessObjectDefinitionDao.getBusinessObjectDefinitionKeysByNamespace(namespaceCode.trim()));
        return businessObjectDefinitionKeys;
    }

    @Override
    public Set<String> getValidFacetFields()
    {
        return ImmutableSet.of(TAG_FACET_FIELD);
    }

    @Override
    public BusinessObjectDefinitionSearchResponse searchBusinessObjectDefinitions(BusinessObjectDefinitionSearchRequest request, Set<String> fields)
    {
        // Validate the business object definition search fields.
        validateSearchResponseFields(fields);

        List<TagEntity> tagEntities = new ArrayList<>();

        if (!CollectionUtils.isEmpty(request.getBusinessObjectDefinitionSearchFilters()))
        {
            // Validate the search request.
            validateBusinessObjectDefinitionSearchRequest(request);

            BusinessObjectDefinitionSearchKey businessObjectDefinitionSearchKey =
                request.getBusinessObjectDefinitionSearchFilters().get(0).getBusinessObjectDefinitionSearchKeys().get(0);

            TagEntity tagEntity = tagDaoHelper.getTagEntity(businessObjectDefinitionSearchKey.getTagKey());

            // If includeTagHierarchy is true, get list of children tag entities down the hierarchy of the specified tag.
            tagEntities.add(tagEntity);
            if (BooleanUtils.isTrue(businessObjectDefinitionSearchKey.isIncludeTagHierarchy()))
            {
                tagEntities.addAll(tagDaoHelper.getTagChildrenEntities(tagEntity));
            }
        }

        // Construct business object search response.
        BusinessObjectDefinitionSearchResponse searchResponse = new BusinessObjectDefinitionSearchResponse();
        List<BusinessObjectDefinition> businessObjectDefinitions = new ArrayList<>();
        searchResponse.setBusinessObjectDefinitions(businessObjectDefinitions);

        // Retrieve all unique business object definition entities and construct a list of business object definitions based on the requested fields.
        for (BusinessObjectDefinitionEntity businessObjectDefinition : ImmutableSet
            .copyOf(businessObjectDefinitionDao.getBusinessObjectDefinitions(tagEntities)))
        {
            businessObjectDefinitions.add(createBusinessObjectDefinitionFromEntity(businessObjectDefinition, fields));
        }

        return searchResponse;
    }


    /**
     * Validates the business object definition update request. This method also trims request parameters.
     *
     * @param request the request.
     *
     * @throws IllegalArgumentException if any validation errors were found.
     */
    private void validateBusinessObjectDefinitionUpdateRequest(BusinessObjectDefinitionUpdateRequest request)
    {
        if (request.getDisplayName() != null)
        {
            request.setDisplayName(request.getDisplayName().trim());
        }

        // Validate attributes.
        attributeHelper.validateAttributes(request.getAttributes());
    }

    /**
     * Validates the business object definition update request. This method also trims request parameters.
     *
     * @param request the request.
     *
     * @throws IllegalArgumentException if any validation errors were found.
     */
    private void validateBusinessObjectDefinitionDescriptiveInformationUpdateRequest(BusinessObjectDefinitionDescriptiveInformationUpdateRequest request)
    {
        if (request.getDisplayName() != null)
        {
            request.setDisplayName(request.getDisplayName().trim());
        }

        if (request.getDescriptiveBusinessObjectFormat() != null)
        {
            DescriptiveBusinessObjectFormatUpdateRequest descriptiveFormat = request.getDescriptiveBusinessObjectFormat();

            descriptiveFormat.setBusinessObjectFormatUsage(
                alternateKeyHelper.validateStringParameter("business object format usage", descriptiveFormat.getBusinessObjectFormatUsage()));
            descriptiveFormat.setBusinessObjectFormatFileType(
                alternateKeyHelper.validateStringParameter("business object format file type", descriptiveFormat.getBusinessObjectFormatFileType()));
        }
    }



    /**
     * Update and persist the business object definition per specified update request.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param request the business object definition update request
     */
    private void updateBusinessObjectDefinitionEntity(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        BusinessObjectDefinitionUpdateRequest request)
    {
        // Update the entity with the new description value.
        businessObjectDefinitionEntity.setDescription(request.getDescription());
        businessObjectDefinitionEntity.setDisplayName(request.getDisplayName());

        // Update the attributes.
        // Load all existing attribute entities in a map with a "lowercase" attribute name as the key for case insensitivity.
        Map<String, BusinessObjectDefinitionAttributeEntity> existingAttributeEntities = new HashMap<>();
        for (BusinessObjectDefinitionAttributeEntity attributeEntity : businessObjectDefinitionEntity.getAttributes())
        {
            String mapKey = attributeEntity.getName().toLowerCase();
            if (existingAttributeEntities.containsKey(mapKey))
            {
                throw new IllegalStateException(String.format(
                    "Found duplicate attribute with name \"%s\" for business object definition {namespace: \"%s\", businessObjectDefinitionName: \"%s\"}.",
                    mapKey, businessObjectDefinitionEntity.getNamespace().getCode(), businessObjectDefinitionEntity.getName()));
            }
            existingAttributeEntities.put(mapKey, attributeEntity);
        }

        // Process the list of attributes to determine that business object definition attribute entities should be created, updated, or deleted.
        List<BusinessObjectDefinitionAttributeEntity> createdAttributeEntities = new ArrayList<>();
        List<BusinessObjectDefinitionAttributeEntity> retainedAttributeEntities = new ArrayList<>();
        if (!CollectionUtils.isEmpty(request.getAttributes()))
        {
            for (Attribute attribute : request.getAttributes())
            {
                // Use a "lowercase" attribute name for case insensitivity.
                String lowercaseAttributeName = attribute.getName().toLowerCase();
                if (existingAttributeEntities.containsKey(lowercaseAttributeName))
                {
                    // Check if the attribute value needs to be updated.
                    BusinessObjectDefinitionAttributeEntity attributeEntity = existingAttributeEntities.get(lowercaseAttributeName);
                    if (!StringUtils.equals(attribute.getValue(), attributeEntity.getValue()))
                    {
                        // Update the business object attribute entity.
                        attributeEntity.setValue(attribute.getValue());
                    }

                    // Add this entity to the list of business object definition attribute entities to be retained.
                    retainedAttributeEntities.add(attributeEntity);
                }
                else
                {
                    // Create a new business object attribute entity.
                    BusinessObjectDefinitionAttributeEntity attributeEntity = new BusinessObjectDefinitionAttributeEntity();
                    businessObjectDefinitionEntity.getAttributes().add(attributeEntity);
                    attributeEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
                    attributeEntity.setName(attribute.getName());
                    attributeEntity.setValue(attribute.getValue());

                    // Add this entity to the list of the newly created business object definition attribute entities.
                    retainedAttributeEntities.add(attributeEntity);
                }
            }
        }

        // Remove any of the currently existing attribute entities that did not get onto the retained entities list.
        businessObjectDefinitionEntity.getAttributes().retainAll(retainedAttributeEntities);

        // Add all of the newly created business object definition attribute entities.
        businessObjectDefinitionEntity.getAttributes().addAll(createdAttributeEntities);

        // Persist the change event entity
        saveBusinessObjectDefinitionChangeEvents(businessObjectDefinitionEntity);

        // Persist the entity.
        businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);
    }

    /**
     * Update and persist the business object definition descriptive information per specified update request.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     * @param request the business object definition update request
     */
    private void updateBusinessObjectDefinitionEntityDescriptiveInformation(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        BusinessObjectDefinitionDescriptiveInformationUpdateRequest request)
    {
        // Update the entity with the new description value.
        businessObjectDefinitionEntity.setDescription(request.getDescription());
        businessObjectDefinitionEntity.setDisplayName(request.getDisplayName());

        // Persist the change event entity
        saveBusinessObjectDefinitionChangeEvents(businessObjectDefinitionEntity);

        businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);
    }

    /**
     * Update and persist the business object definition change events
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     */
    private void saveBusinessObjectDefinitionChangeEvents(BusinessObjectDefinitionEntity businessObjectDefinitionEntity)
    {
       businessObjectDefinitionDaoHelper.saveBusinessObjectDefinitionChangeEvents(businessObjectDefinitionEntity);
    }

    /**
     * Creates a business object definition from the persisted entity.
     *
     * @param businessObjectDefinitionEntity the business object definition entity
     *
     * @return the business object definition
     */
    private BusinessObjectDefinition createBusinessObjectDefinitionFromEntity(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        Boolean includeBusinessObjectDefinitionUpdateHistory)
    {
        // Create a business object definition.
        BusinessObjectDefinition businessObjectDefinition = new BusinessObjectDefinition();
        businessObjectDefinition.setId(businessObjectDefinitionEntity.getId());
        businessObjectDefinition.setNamespace(businessObjectDefinitionEntity.getNamespace().getCode());
        businessObjectDefinition.setBusinessObjectDefinitionName(businessObjectDefinitionEntity.getName());
        businessObjectDefinition.setDescription(businessObjectDefinitionEntity.getDescription());
        businessObjectDefinition.setDataProviderName(businessObjectDefinitionEntity.getDataProvider().getName());
        businessObjectDefinition.setDisplayName(businessObjectDefinitionEntity.getDisplayName());

        // Add attributes.
        List<Attribute> attributes = new ArrayList<>();
        businessObjectDefinition.setAttributes(attributes);
        for (BusinessObjectDefinitionAttributeEntity attributeEntity : businessObjectDefinitionEntity.getAttributes())
        {
            attributes.add(new Attribute(attributeEntity.getName(), attributeEntity.getValue()));
        }

        if (businessObjectDefinitionEntity.getDescriptiveBusinessObjectFormat() != null)
        {
            BusinessObjectFormatEntity descriptiveFormatEntity = businessObjectDefinitionEntity.getDescriptiveBusinessObjectFormat();
            DescriptiveBusinessObjectFormat descriptiveBusinessObjectFormat = new DescriptiveBusinessObjectFormat();
            businessObjectDefinition.setDescriptiveBusinessObjectFormat(descriptiveBusinessObjectFormat);
            descriptiveBusinessObjectFormat.setBusinessObjectFormatUsage(descriptiveFormatEntity.getUsage());
            descriptiveBusinessObjectFormat.setBusinessObjectFormatFileType(descriptiveFormatEntity.getFileType().getCode());
            descriptiveBusinessObjectFormat.setBusinessObjectFormatVersion(descriptiveFormatEntity.getBusinessObjectFormatVersion());
        }

        // Add sample data files.
        List<SampleDataFile> sampleDataFiles = new ArrayList<>();
        businessObjectDefinition.setSampleDataFiles(sampleDataFiles);
        for (BusinessObjectDefinitionSampleDataFileEntity sampleDataFileEntity : businessObjectDefinitionEntity.getSampleDataFiles())
        {
            sampleDataFiles.add(new SampleDataFile(sampleDataFileEntity.getDirectoryPath(), sampleDataFileEntity.getFileName()));
        }

        // Add auditable fields.
        businessObjectDefinition.setCreatedByUserId(businessObjectDefinitionEntity.getCreatedBy());
        businessObjectDefinition.setLastUpdatedByUserId(businessObjectDefinitionEntity.getUpdatedBy());
        businessObjectDefinition.setLastUpdatedOn(HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionEntity.getUpdatedOn()));

        // Add change events.
        final List<BusinessObjectDefinitionChangeEvent> businessObjectDefinitionChangeEvents = new ArrayList<>();
        if (BooleanUtils.isTrue(includeBusinessObjectDefinitionUpdateHistory))
        {
            businessObjectDefinitionEntity.getChangeEvents().forEach(businessObjectDefinitionChangeEventEntity ->
            {
                DescriptiveBusinessObjectFormatUpdateRequest descriptiveBusinessObjectFormatUpdateRequest = null;
                if (businessObjectDefinitionChangeEventEntity.getFileType() != null)
                {
                    descriptiveBusinessObjectFormatUpdateRequest =
                        new DescriptiveBusinessObjectFormatUpdateRequest(businessObjectDefinitionChangeEventEntity.getUsage(),
                            businessObjectDefinitionChangeEventEntity.getFileType());
                }
                businessObjectDefinitionChangeEvents.add(new BusinessObjectDefinitionChangeEvent(businessObjectDefinitionChangeEventEntity.getDisplayName(),
                    businessObjectDefinitionChangeEventEntity.getDescription(), descriptiveBusinessObjectFormatUpdateRequest,
                    HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionChangeEventEntity.getCreatedOn()),
                    businessObjectDefinitionChangeEventEntity.getCreatedBy()));
            });
        }
        businessObjectDefinition.setBusinessObjectDefinitionChangeEvents(businessObjectDefinitionChangeEvents);
        return businessObjectDefinition;
    }

    /**
     * Creates a light-weight business object definition from its persisted entity based on a set of requested fields.
     *
     * @param businessObjectDefinitionEntity the specified business object definition entity
     * @param fields the set of requested fields
     *
     * @return the light-weight business object definition
     */
    private BusinessObjectDefinition createBusinessObjectDefinitionFromEntity(BusinessObjectDefinitionEntity businessObjectDefinitionEntity, Set<String> fields)
    {
        BusinessObjectDefinition definition = new BusinessObjectDefinition();

        // Populate namespace and business object definition name fields by default
        definition.setNamespace(businessObjectDefinitionEntity.getNamespace().getCode());
        definition.setBusinessObjectDefinitionName(businessObjectDefinitionEntity.getName());

        // Decorate object with only the required fields
        if (fields.contains(DATA_PROVIDER_NAME_FIELD))
        {
            definition.setDataProviderName(businessObjectDefinitionEntity.getDataProvider().getName());
        }

        if (fields.contains(SHORT_DESCRIPTION_FIELD))
        {
            // Get the configured value for short description's length
            Integer shortDescMaxLength = configurationHelper.getProperty(ConfigurationValue.BUSINESS_OBJECT_DEFINITION_SHORT_DESCRIPTION_LENGTH, Integer.class);

            definition.setShortDescription(HerdStringUtils.getShortDescription(businessObjectDefinitionEntity.getDescription(), shortDescMaxLength));
        }

        if (fields.contains(DISPLAY_NAME_FIELD))
        {
            definition.setDisplayName(businessObjectDefinitionEntity.getDisplayName());
        }

        return definition;
    }

    @Override
    public Set<String> getValidSearchResponseFields()
    {
        return ImmutableSet.of(DATA_PROVIDER_NAME_FIELD, SHORT_DESCRIPTION_FIELD, DISPLAY_NAME_FIELD);
    }

    /**
     * Validate the business object definition search request. This method also trims the request parameters.
     *
     * @param businessObjectDefinitionSearchRequest the business object definition search request
     */
    private void validateBusinessObjectDefinitionSearchRequest(BusinessObjectDefinitionSearchRequest businessObjectDefinitionSearchRequest)
    {
        if (CollectionUtils.size(businessObjectDefinitionSearchRequest.getBusinessObjectDefinitionSearchFilters()) == 1 &&
            businessObjectDefinitionSearchRequest.getBusinessObjectDefinitionSearchFilters().get(0) != null)
        {
            // Get the business object definition search filter.
            BusinessObjectDefinitionSearchFilter businessObjectDefinitionSearchFilter =
                businessObjectDefinitionSearchRequest.getBusinessObjectDefinitionSearchFilters().get(0);

            Assert.isTrue(CollectionUtils.size(businessObjectDefinitionSearchFilter.getBusinessObjectDefinitionSearchKeys()) == 1 &&
                    businessObjectDefinitionSearchFilter.getBusinessObjectDefinitionSearchKeys().get(0) != null,
                "Exactly one business object definition search key must be specified.");

            // Get the tag search key.
            BusinessObjectDefinitionSearchKey businessObjectDefinitionSearchKey =
                businessObjectDefinitionSearchFilter.getBusinessObjectDefinitionSearchKeys().get(0);

            tagHelper.validateTagKey(businessObjectDefinitionSearchKey.getTagKey());
        }
        else
        {
            Assert.isTrue(CollectionUtils.size(businessObjectDefinitionSearchRequest.getBusinessObjectDefinitionSearchFilters()) == 1 &&
                    businessObjectDefinitionSearchRequest.getBusinessObjectDefinitionSearchFilters().get(0) != null,
                "Exactly one business object definition search filter must be specified.");
        }
    }

    @Override
    public void updateBusinessObjectDefinitionEntitySampleFile(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        BusinessObjectDefinitionSampleFileUpdateDto businessObjectDefinitionSampleFileUpdateDto)
    {
        String path = businessObjectDefinitionSampleFileUpdateDto.getPath();
        String fileName = businessObjectDefinitionSampleFileUpdateDto.getFileName();
        long fileSize = businessObjectDefinitionSampleFileUpdateDto.getFileSize();

        // Validate business object key
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);
        // Validate file name
        Assert.hasText(fileName, "A file name must be specified.");
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);
        Collection<BusinessObjectDefinitionSampleDataFileEntity> sampleFiles = businessObjectDefinitionEntity.getSampleDataFiles();
        boolean found = false;
        for (BusinessObjectDefinitionSampleDataFileEntity sampleDataFieEntity : sampleFiles)
        {
            // Assume the path is the same for this business object definition
            if (sampleDataFieEntity.getFileName().equals(fileName))
            {
                found = true;
                sampleDataFieEntity.setFileSizeBytes(fileSize);
                businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);
                break;
            }
        }
        // Create a new entity when not found
        if (!found)
        {
            StorageEntity storageEntity = storageDaoHelper.getStorageEntity(StorageEntity.SAMPLE_DATA_FILE_STORAGE);
            BusinessObjectDefinitionSampleDataFileEntity sampleDataFileEntity = new BusinessObjectDefinitionSampleDataFileEntity();
            sampleDataFileEntity.setStorage(storageEntity);
            sampleDataFileEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
            sampleDataFileEntity.setDirectoryPath(path);
            sampleDataFileEntity.setFileName(fileName);
            sampleDataFileEntity.setFileSizeBytes(fileSize);
            businessObjectDefinitionEntity.getSampleDataFiles().add(sampleDataFileEntity);
            businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);
        }

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation starts a new transaction.
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void updateSearchIndexDocumentBusinessObjectDefinition(SearchIndexUpdateDto searchIndexUpdateDto)
    {
        updateSearchIndexDocumentBusinessObjectDefinitionImpl(searchIndexUpdateDto);
    }

    /**
     * Updates the search index document representation of the business object definition.
     *
     * @param searchIndexUpdateDto the SearchIndexUpdateDto object
     */
    protected void updateSearchIndexDocumentBusinessObjectDefinitionImpl(SearchIndexUpdateDto searchIndexUpdateDto)
    {
        final String indexName = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_INDEX_NAME, String.class);
        final String documentType = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);

        String modificationType = searchIndexUpdateDto.getModificationType();
        List<Integer> ids = searchIndexUpdateDto.getBusinessObjectDefinitionIds();

        // Start at index 0
        int fromIndex = 0;

        // Process documents until the ids are all updated
        while (fromIndex < ids.size())
        {
            // Process based on a document chunk size, if the id.size is greater than the chunk size then use the chunk size
            int toIndex = ids.size() > fromIndex + UPDATE_SEARCH_INDEX_DOCUMENT_CHUNK_SIZE ? fromIndex + UPDATE_SEARCH_INDEX_DOCUMENT_CHUNK_SIZE : ids.size();

            // Switch on the type of CRUD modification to be done
            switch (modificationType)
            {
                case SEARCH_INDEX_UPDATE_TYPE_CREATE:
                    // Create a search index document
                    indexFunctionsDao.createIndexDocuments(indexName, documentType, convertBusinessObjectDefinitionEntityListToJSONStringMap(
                        businessObjectDefinitionDao.getAllBusinessObjectDefinitionsByIds(ids.subList(fromIndex, toIndex))));
                    break;
                case SEARCH_INDEX_UPDATE_TYPE_UPDATE:
                    // Update a search index document
                    indexFunctionsDao.updateIndexDocuments(indexName, documentType, convertBusinessObjectDefinitionEntityListToJSONStringMap(
                        businessObjectDefinitionDao.getAllBusinessObjectDefinitionsByIds(ids.subList(fromIndex, toIndex))));
                    break;
                case SEARCH_INDEX_UPDATE_TYPE_DELETE:
                    // Delete a search index document
                    indexFunctionsDao.deleteIndexDocuments(indexName, documentType, ids.subList(fromIndex, toIndex));
                    break;
                default:
                    LOGGER.warn("Unknown modification type received.");
                    break;
            }

            // Set the from index to the toIndex
            fromIndex = toIndex;
        }

        LOGGER.info("Finished processing {} documents", ids.size());
    }

    /**
     * Private method to convert a business object definition entity list to a list of JSON strings.
     *
     * @param businessObjectDefinitionEntities the list of business object definitions
     *
     * @return Map of key, business object definition ids, and value, business object definition entity as JSON string
     */
    private Map<String, String> convertBusinessObjectDefinitionEntityListToJSONStringMap(List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities)
    {
        LOGGER.info("List size is {}.", businessObjectDefinitionEntities.size());

        Map<String, String> businessObjectDefinitionJSONMap = new HashMap<>();

        businessObjectDefinitionEntities.forEach(businessObjectDefinitionEntity ->
        {
            // Fetch Join with .size()
            businessObjectDefinitionEntity.getAttributes().size();
            businessObjectDefinitionEntity.getBusinessObjectDefinitionTags().size();
            businessObjectDefinitionEntity.getBusinessObjectFormats().size();
            businessObjectDefinitionEntity.getColumns().size();
            businessObjectDefinitionEntity.getSampleDataFiles().size();

            String jsonString = businessObjectDefinitionHelper.safeObjectMapperWriteValueAsString(businessObjectDefinitionEntity);

            if (StringUtils.isNotEmpty(jsonString))
            {
                businessObjectDefinitionJSONMap.put(businessObjectDefinitionEntity.getId().toString(), jsonString);
            }
            else
            {
                LOGGER.warn("Business Object Definition Entity {} jsonString is empty.", businessObjectDefinitionEntity.getId());
            }
        });

        return businessObjectDefinitionJSONMap;
    }
}
