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

import java.math.BigDecimal;
import java.util.ArrayList;
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
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.IndexFunctionsDao;
import org.finra.herd.dao.TagDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.Tag;
import org.finra.herd.model.api.xml.TagChild;
import org.finra.herd.model.api.xml.TagCreateRequest;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.api.xml.TagListResponse;
import org.finra.herd.model.api.xml.TagSearchFilter;
import org.finra.herd.model.api.xml.TagSearchKey;
import org.finra.herd.model.api.xml.TagSearchRequest;
import org.finra.herd.model.api.xml.TagSearchResponse;
import org.finra.herd.model.api.xml.TagTypeKey;
import org.finra.herd.model.api.xml.TagUpdateRequest;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.SearchIndexUpdateDto;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.SearchIndexTypeEntity;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.model.jpa.TagTypeEntity;
import org.finra.herd.service.SearchableService;
import org.finra.herd.service.TagService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.SearchIndexUpdateHelper;
import org.finra.herd.dao.helper.TagDaoHelper;
import org.finra.herd.service.helper.TagHelper;
import org.finra.herd.service.helper.TagTypeDaoHelper;

/**
 * The tag service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class TagServiceImpl implements TagService, SearchableService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TagServiceImpl.class);

    // Constant to hold the description field option for the search response.
    public final static String DESCRIPTION_FIELD = "description".toLowerCase();

    // Constant to hold the display name field option for the search response.
    public final static String DISPLAY_NAME_FIELD = "displayName".toLowerCase();

    // Constant to hold the has children field option for the search response.
    public final static String HAS_CHILDREN_FIELD = "hasChildren".toLowerCase();

    // Constant to hold the parent tag key field option for the search response.
    public final static String PARENT_TAG_KEY_FIELD = "parentTagKey".toLowerCase();

    // Constant to hold the search score multiplier option for the search response.
    public final static String SEARCH_SCORE_MULTIPLIER_FIELD = "searchScoreMultiplier".toLowerCase();

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private IndexFunctionsDao indexFunctionsDao;

    @Autowired
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

    @Autowired
    private TagDao tagDao;

    @Autowired
    private TagDaoHelper tagDaoHelper;

    @Autowired
    private TagHelper tagHelper;

    @Autowired
    private TagTypeDaoHelper tagTypeDaoHelper;

    @Override
    public Tag createTag(TagCreateRequest request)
    {
        // Validate and trim the request parameters.
        validateTagCreateRequest(request);

        // Get the tag type and ensure it exists.
        TagTypeEntity tagTypeEntity = tagTypeDaoHelper.getTagTypeEntity(new TagTypeKey(request.getTagKey().getTagTypeCode()));

        // Validate that the tag entity does not already exist.
        if (tagDao.getTagByKey(request.getTagKey()) != null)
        {
            throw new AlreadyExistsException(String
                .format("Unable to create tag with tag type code \"%s\" and tag code \"%s\" because it already exists.", request.getTagKey().getTagTypeCode(),
                    request.getTagKey().getTagCode()));
        }

        // List of tag entities to update in the search index
        List<TagEntity> tagEntities = new ArrayList<>();

        // Validate that the specified display name does not already exist for the specified tag type
        tagDaoHelper.assertDisplayNameDoesNotExistForTag(request.getTagKey().getTagTypeCode(), request.getDisplayName());
        TagEntity parentTagEntity = null;
        if (request.getParentTagKey() != null)
        {
            parentTagEntity = tagDaoHelper.getTagEntity(request.getParentTagKey());

            // Add the parent tag entity to the list of tag entities to update in the search index
            tagEntities.add(parentTagEntity);
        }

        // Create and persist a new tag entity from the information in the request.
        TagEntity tagEntity = createTagEntity(request, tagTypeEntity, parentTagEntity);

        // Notify the tag search index that a tag must be created.
        tagEntities.add(tagEntity);
        searchIndexUpdateHelper.modifyTagsInSearchIndex(tagEntities, SEARCH_INDEX_UPDATE_TYPE_CREATE);

        // Create and return the tag object from the persisted entity.
        return createTagFromEntity(tagEntity);
    }

    @Override
    public Tag deleteTag(TagKey tagKey)
    {
        // Validate and trim the tag key.
        tagHelper.validateTagKey(tagKey);

        // Retrieve and ensure that a Tag already exists for the given tag key.
        TagEntity tagEntity = tagDaoHelper.getTagEntity(tagKey);

        // List of tag entities to update in the search index
        List<TagEntity> tagEntities = new ArrayList<>();
        tagEntities.add(tagEntity);

        // If there is a parent tag entity add it to the tag entities list
        if (tagEntity.getParentTagEntity() != null)
        {
            tagEntities.add(tagEntity.getParentTagEntity());
        }

        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities = businessObjectDefinitionDao.getBusinessObjectDefinitions(tagEntities);

        // delete the tag.
        tagDao.delete(tagEntity);

        // Notify the tag search index that a tag must be deleted.
        searchIndexUpdateHelper.modifyTagInSearchIndex(tagEntity, SEARCH_INDEX_UPDATE_TYPE_DELETE);

        // If there is a parent tag entity, notify the tag search index that the parent tag must be updated
        if (tagEntity.getParentTagEntity() != null)
        {
            searchIndexUpdateHelper.modifyTagInSearchIndex(tagEntity.getParentTagEntity(), SEARCH_INDEX_UPDATE_TYPE_UPDATE);
        }

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionsInSearchIndex(businessObjectDefinitionEntities, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Create and return the tag object from the deleted entity.
        return createTagFromEntity(tagEntity);
    }

    @Override
    public Tag getTag(TagKey tagKey)
    {
        // Perform validation and trim.
        tagHelper.validateTagKey(tagKey);

        // Retrieve and ensure that a Tag already exists with the specified key.
        TagEntity tagEntity = tagDaoHelper.getTagEntity(tagKey);

        // Create and return the tag object from the entity which was retrieved.
        return createTagFromEntity(tagEntity);
    }

    @Override
    public TagListResponse getTags(String tagTypeCode, String tagCode)
    {
        // Validate and trim the tag type code.
        String tagTypeCodeLocal = alternateKeyHelper.validateStringParameter("tag type code", tagTypeCode);

        String cleanTagCode = tagCode;

        // Retrieve and ensure that a tag type exists for the specified tag type code.
        tagTypeDaoHelper.getTagTypeEntity(new TagTypeKey(tagTypeCodeLocal));

        // Get the list of tag keys.
        TagListResponse response = new TagListResponse();

        //not root, need to set the tag key and parent tag key
        //getTag method will validate the requested tag exists
        if (tagCode != null)
        {
            cleanTagCode = alternateKeyHelper.validateStringParameter("tag code", tagCode);
            TagKey tagKey = new TagKey(tagTypeCodeLocal, cleanTagCode);
            Tag tag = getTag(tagKey);
            response.setTagKey(tag.getTagKey());
            response.setParentTagKey(tag.getParentTagKey());
        }

        List<TagChild> tagChildren = tagDao.getTagsByTagTypeAndParentTagCode(tagTypeCodeLocal, cleanTagCode);

        response.setTagChildren(tagChildren);

        return response;
    }

    @Override
    public Set<String> getValidSearchResponseFields()
    {
        return ImmutableSet.of(DISPLAY_NAME_FIELD, SEARCH_SCORE_MULTIPLIER_FIELD, DESCRIPTION_FIELD, PARENT_TAG_KEY_FIELD, HAS_CHILDREN_FIELD);
    }

    @Override
    public TagSearchResponse searchTags(TagSearchRequest request, Set<String> fields)
    {
        // Validate and trim the request parameters.
        validateTagSearchRequest(request);

        // Validate and trim the search response fields.
        validateSearchResponseFields(fields);

        // Prepare the result list.
        List<TagEntity> tagEntities = new ArrayList<>();

        // If search key is specified, use it to retrieve the tags.
        if (CollectionUtils.isNotEmpty(request.getTagSearchFilters()) && request.getTagSearchFilters().get(0) != null)
        {
            // Get the tag search key.
            TagSearchKey tagSearchKey = request.getTagSearchFilters().get(0).getTagSearchKeys().get(0);

            // Retrieve and ensure that a tag type exists for the specified tag type code.
            TagTypeEntity tagTypeEntity = tagTypeDaoHelper.getTagTypeEntity(new TagTypeKey(tagSearchKey.getTagTypeCode()));

            // Retrieve the tags.
            tagEntities.addAll(tagDao.getTagsByTagTypeEntityAndParentTagCode(tagTypeEntity, tagSearchKey.getParentTagCode(), tagSearchKey.isIsParentTagNull()));
        }
        // The search key is not specified, so select all tags registered in the system.
        else
        {
            // Retrieve the tags.
            tagEntities.addAll(tagDao.getTags());
        }

        // Build the list of tags.
        List<Tag> tags = new ArrayList<>();
        for (TagEntity tagEntity : tagEntities)
        {
            tags.add(createTagFromEntity(tagEntity, false, fields.contains(DISPLAY_NAME_FIELD), fields.contains(SEARCH_SCORE_MULTIPLIER_FIELD),
                fields.contains(DESCRIPTION_FIELD), false, false, false, fields.contains(PARENT_TAG_KEY_FIELD), fields.contains(HAS_CHILDREN_FIELD)));
        }

        // Build and return the tag search response.
        return new TagSearchResponse(tags);
    }

    @Override
    public void updateSearchIndexDocumentTag(SearchIndexUpdateDto searchIndexUpdateDto)
    {
        final String indexName = SearchIndexTypeEntity.SearchIndexTypes.TAG.name().toLowerCase();
        final String documentType = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);

        String modificationType = searchIndexUpdateDto.getModificationType();
        List<Integer> ids = searchIndexUpdateDto.getTagIds();

        // Switch on the type of CRUD modification to be done
        switch (modificationType)
        {
            case SEARCH_INDEX_UPDATE_TYPE_CREATE:
                // Create a search index document
            indexFunctionsDao.createIndexDocuments(indexName, documentType, convertTagEntityListToJSONStringMap(tagDao.getTagsByIds(ids)));
                break;
            case SEARCH_INDEX_UPDATE_TYPE_UPDATE:
                // Update a search index document
               indexFunctionsDao.updateIndexDocuments(indexName, documentType, convertTagEntityListToJSONStringMap(tagDao.getTagsByIds(ids)));
                break;
            case SEARCH_INDEX_UPDATE_TYPE_DELETE:
                // Delete a search index document
                indexFunctionsDao.deleteIndexDocuments(indexName, documentType, ids);
                break;
            default:
                LOGGER.warn("Unknown modification type received.");
                break;
        }
    }

    @Override
    public Tag updateTag(TagKey tagKey, TagUpdateRequest tagUpdateRequest)
    {
        // Perform validation and trim
        tagHelper.validateTagKey(tagKey);

        // Perform validation and trim.
        validateTagUpdateRequest(tagKey, tagUpdateRequest);

        // Retrieve and ensure that a tag already exists with the specified key.
        TagEntity tagEntity = tagDaoHelper.getTagEntity(tagKey);

        // Validate the display name does not already exist for another tag for this tag type in the database.
        if (!StringUtils.equalsIgnoreCase(tagEntity.getDisplayName(), tagUpdateRequest.getDisplayName()))
        {
            // Validate that the description is different.
            tagDaoHelper.assertDisplayNameDoesNotExistForTag(tagKey.getTagTypeCode(), tagUpdateRequest.getDisplayName());
        }

        // List of tag entities to update in the search index
        List<TagEntity> tagEntities = new ArrayList<>();
        tagEntities.add(tagEntity);

        // If there is an original tag entity parent, then update
        if (tagEntity.getParentTagEntity() != null)
        {
            tagEntities.add(tagEntity.getParentTagEntity());
        }

        // Validate the parent tag if one specified.
        TagEntity parentTagEntity = null;
        if (tagUpdateRequest.getParentTagKey() != null)
        {
            // Get parent tag entity and ensure it exists.
            parentTagEntity = tagDaoHelper.getTagEntity(tagUpdateRequest.getParentTagKey());

            // Validate the parent tag entity.
            tagDaoHelper.validateParentTagEntity(tagEntity, parentTagEntity);

            // Add the parent tag entity to the tag entities list
            tagEntities.add(parentTagEntity);
        }

        // Update and persist the tag entity.
        updateTagEntity(tagEntity, tagUpdateRequest, parentTagEntity);

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionsInSearchIndex(businessObjectDefinitionDao.getBusinessObjectDefinitions(tagEntities),
            SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Notify the tag search index that tags must be updated.
        searchIndexUpdateHelper.modifyTagsInSearchIndex(tagEntities, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Create and return the tag object from the tag entity.
        return createTagFromEntity(tagEntity);
    }

    /**
     * Creates and persists a new Tag entity.
     *
     * @param request the tag create request
     * @param tagTypeEntity the specified tag type entity.
     * @param parentTagEntity the specified parent tag entity
     *
     * @return the newly created tag entity.
     */
    private TagEntity createTagEntity(TagCreateRequest request, TagTypeEntity tagTypeEntity, TagEntity parentTagEntity)
    {
        TagEntity tagEntity = new TagEntity();

        tagEntity.setTagType(tagTypeEntity);
        tagEntity.setTagCode(request.getTagKey().getTagCode());
        tagEntity.setDisplayName(request.getDisplayName());
        tagEntity.setSearchScoreMultiplier(request.getSearchScoreMultiplier());
        tagEntity.setDescription(request.getDescription());
        tagEntity.setParentTagEntity(parentTagEntity);

        return tagDao.saveAndRefresh(tagEntity);
    }

    /**
     * Creates the tag from the persisted entity.
     *
     * @param tagEntity the tag entity
     *
     * @return the tag
     */
    private Tag createTagFromEntity(TagEntity tagEntity)
    {
        return createTagFromEntity(tagEntity, true, true, true, true, true, true, true, true, false);
    }

    /**
     * Creates the tag from the persisted entity.
     *
     * @param tagEntity the tag entity
     * @param includeId specifies to include the display name field
     * @param includeDisplayName specifies to include the display name field
     * @param includeSearchScoreMultiplier specifies to include the search score multiplier
     * @param includeDescription specifies to include the description field
     * @param includeUserId specifies to include the user id of the user who created this tag
     * @param includeLastUpdatedByUserId specifies to include the user id of the user who last updated this tag
     * @param includeUpdatedTime specifies to include the timestamp of when this tag is last updated
     * @param includeParentTagKey specifies to include the parent tag key field
     * @param includeHasChildren specifies to include the hasChildren field
     *
     * @return the tag
     */
    private Tag createTagFromEntity(TagEntity tagEntity, boolean includeId, boolean includeDisplayName, boolean includeSearchScoreMultiplier,
        boolean includeDescription, boolean includeUserId, boolean includeLastUpdatedByUserId, boolean includeUpdatedTime, boolean includeParentTagKey,
        boolean includeHasChildren)
    {
        Tag tag = new Tag();

        if (includeId)
        {
            tag.setId(tagEntity.getId());
        }

        tag.setTagKey(new TagKey(tagEntity.getTagType().getCode(), tagEntity.getTagCode()));

        if (includeDisplayName)
        {
            tag.setDisplayName(tagEntity.getDisplayName());
        }

        if (includeSearchScoreMultiplier)
        {
            tag.setSearchScoreMultiplier(tagEntity.getSearchScoreMultiplier());
        }

        if (includeDescription)
        {
            tag.setDescription(tagEntity.getDescription());
        }

        if (includeUserId)
        {
            tag.setUserId(tagEntity.getCreatedBy());
        }

        if (includeLastUpdatedByUserId)
        {
            tag.setLastUpdatedByUserId(tagEntity.getUpdatedBy());
        }

        if (includeUpdatedTime)
        {
            tag.setUpdatedTime(HerdDateUtils.getXMLGregorianCalendarValue(tagEntity.getUpdatedOn()));
        }

        if (includeParentTagKey)
        {
            TagEntity parentTagEntity = tagEntity.getParentTagEntity();
            if (parentTagEntity != null)
            {
                tag.setParentTagKey(new TagKey(parentTagEntity.getTagType().getCode(), parentTagEntity.getTagCode()));
            }
        }

        if (includeHasChildren)
        {
            tag.setHasChildren(!tagEntity.getChildrenTagEntities().isEmpty());
        }

        return tag;
    }

    /**
     * Private method to convert a tag entity list to a list of JSON strings.
     *
     * @param tagEntities the list of tags
     *
     * @return Map of key, business object definition ids, and value, business object definition entity as JSON string
     */
    private Map<String, String> convertTagEntityListToJSONStringMap(List<TagEntity> tagEntities)
    {
        Map<String, String> tagJSONMap = new HashMap<>();

        tagEntities.forEach(tagEntity -> {
            String jsonString = tagHelper.safeObjectMapperWriteValueAsString(tagEntity);

            if (StringUtils.isNotEmpty(jsonString))
            {
                tagJSONMap.put(tagEntity.getId().toString(), jsonString);
            }
        });

        return tagJSONMap;
    }

    /**
     * Updates and persists the tag entity per the specified update request.
     *
     * @param tagEntity the tag entity
     * @param request the tag update request
     * @param parentTagEntity the parent tag entity, maybe null
     */
    private void updateTagEntity(TagEntity tagEntity, TagUpdateRequest request, TagEntity parentTagEntity)
    {
        tagEntity.setDisplayName(request.getDisplayName());
        tagEntity.setSearchScoreMultiplier(request.getSearchScoreMultiplier());
        tagEntity.setDescription(request.getDescription());
        tagEntity.setParentTagEntity(parentTagEntity);
        tagDao.saveAndRefresh(tagEntity);
    }

    /**
     * Validate the tag create request. This method also trims the request parameters.
     *
     * @param request the tag create request
     */
    private void validateTagCreateRequest(TagCreateRequest request)
    {
        Assert.notNull(request, "A tag create request must be specified.");
        tagHelper.validateTagKey(request.getTagKey());

        if (request.getParentTagKey() != null)
        {
            tagHelper.validateTagKey(request.getParentTagKey());
            tagDaoHelper.validateParentTagType(request.getTagKey().getTagTypeCode(), request.getParentTagKey().getTagTypeCode());
        }

        request.setDisplayName(alternateKeyHelper.validateStringParameter("display name", request.getDisplayName()));

        validateTagSearchScoreMultiplier(request.getSearchScoreMultiplier());
    }

    /**
     * Validate an optional tag's search score multiplier value.
     *
     * @param searchScoreMultiplier the tag's search score multiplier value
     */
    private void validateTagSearchScoreMultiplier(BigDecimal searchScoreMultiplier)
    {
        if (searchScoreMultiplier != null && searchScoreMultiplier.compareTo(BigDecimal.ZERO) == -1)
        {
            throw new IllegalArgumentException(
                String.format("The searchScoreMultiplier can not have a negative value. searchScoreMultiplier=%s", searchScoreMultiplier.toPlainString()));
        }
    }

    /**
     * Validate the tag search request. This method also trims the request parameters.
     *
     * @param tagSearchRequest the tag search request
     */
    private void validateTagSearchRequest(TagSearchRequest tagSearchRequest)
    {
        Assert.notNull(tagSearchRequest, "A tag search request must be specified.");

        // Continue validation if the list of tag search filters is not empty.
        if (CollectionUtils.isNotEmpty(tagSearchRequest.getTagSearchFilters()) && tagSearchRequest.getTagSearchFilters().get(0) != null)
        {
            // Validate that there is only one tag search filter.
            Assert.isTrue(CollectionUtils.size(tagSearchRequest.getTagSearchFilters()) == 1, "At most one tag search filter must be specified.");

            // Get the tag search filter.
            TagSearchFilter tagSearchFilter = tagSearchRequest.getTagSearchFilters().get(0);

            // Validate that exactly one tag search key is specified.
            Assert.isTrue(CollectionUtils.size(tagSearchFilter.getTagSearchKeys()) == 1 && tagSearchFilter.getTagSearchKeys().get(0) != null,
                "Exactly one tag search key must be specified.");

            // Get the tag search key.
            TagSearchKey tagSearchKey = tagSearchFilter.getTagSearchKeys().get(0);

            tagSearchKey.setTagTypeCode(alternateKeyHelper.validateStringParameter("tag type code", tagSearchKey.getTagTypeCode()));

            if (tagSearchKey.getParentTagCode() != null)
            {
                tagSearchKey.setParentTagCode(tagSearchKey.getParentTagCode().trim());
            }

            // Fail validation when parent tag code is specified along with the isParentTagNull flag set to true.
            Assert.isTrue(StringUtils.isBlank(tagSearchKey.getParentTagCode()) || BooleanUtils.isNotTrue(tagSearchKey.isIsParentTagNull()),
                "A parent tag code can not be specified when isParentTagNull flag is set to true.");
        }
    }

    /**
     * Validates the tag update request. This method also trims the request parameters.
     *
     * @param tagKey the tag key
     * @param request the specified tag update request
     */
    private void validateTagUpdateRequest(TagKey tagKey, TagUpdateRequest request)
    {
        Assert.notNull(request, "A tag update request must be specified.");

        if (request.getParentTagKey() != null)
        {
            tagHelper.validateTagKey(request.getParentTagKey());
            tagDaoHelper.validateParentTagType(tagKey.getTagTypeCode(), request.getParentTagKey().getTagTypeCode());
        }

        request.setDisplayName(alternateKeyHelper.validateStringParameter("display name", request.getDisplayName()));

        validateTagSearchScoreMultiplier(request.getSearchScoreMultiplier());
    }

    @Override
    public boolean indexSizeCheckValidationTags(String indexName)
    {
        final String documentType = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);

        // Simple count validation, index size should equal entity list size
        final long indexSize = indexFunctionsDao.getNumberOfTypesInIndex(indexName, documentType);
            //searchFunctions.getNumberOfTypesInIndexFunction().apply(indexName, documentType);
        final long tagDatabaseTableSize = tagDao.getCountOfAllTags();
        if (tagDatabaseTableSize != indexSize)
        {
            LOGGER.error("Index validation failed, tag database table size {}, does not equal index size {}.", tagDatabaseTableSize, indexSize);
        }

        return tagDatabaseTableSize == indexSize;
    }

    @Override
    public boolean indexSpotCheckPercentageValidationTags(String indexName)
    {
        final Double spotCheckPercentage = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_PERCENTAGE, Double.class);

        // Get a list of all tags
        final List<TagEntity> tagEntityList = Collections.unmodifiableList(tagDao.getPercentageOfAllTags(spotCheckPercentage));

        return indexValidateTagsList(tagEntityList);
    }

    @Override
    public boolean indexSpotCheckMostRecentValidationTags(String indexName)
    {
        final Integer spotCheckMostRecentNumber =
            configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_TAG_SPOT_CHECK_MOST_RECENT_NUMBER, Integer.class);

        // Get a list of all tags
        final List<TagEntity> tagEntityList = Collections.unmodifiableList(tagDao.getMostRecentTags(spotCheckMostRecentNumber));

        return indexValidateTagsList(tagEntityList);
    }

    @Override
    @Async
    public Future<Void> indexValidateAllTags(String indexName)
    {
        final String documentType = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);

        // Get a list of all tags
        final List<TagEntity> tagEntityList = Collections.unmodifiableList(tagDao.getTags());

        // Remove any index documents that are not in the database
        removeAnyIndexDocumentsThatAreNotInTagsList(indexName, documentType, tagEntityList);

        // Validate all Tags
        tagHelper.executeFunctionForTagEntities(indexName, documentType, tagEntityList, indexFunctionsDao::validateDocumentIndex);

        // Return an AsyncResult so callers will know the future is "done". They can call "isDone" to know when this method has completed and they
        // can call "get" to see if any exceptions were thrown.
        return new AsyncResult<>(null);
    }

    /**
     * Method to remove tags in the index that don't exist in the database
     *
     * @param indexName the name of the index
     * @param documentType the document type
     * @param tagEntityList list of tags in the database
     */
    private void removeAnyIndexDocumentsThatAreNotInTagsList(final String indexName, final String documentType, List<TagEntity> tagEntityList)
    {
        // Get a list of tag ids from the list of tag entities in the database
        List<String> databaseTagIdList = new ArrayList<>();
        tagEntityList.forEach(tagEntity -> databaseTagIdList.add(tagEntity.getId().toString()));

        // Get a list of tag ids in the search index
        List<String> indexDocumentTagIdList = indexFunctionsDao.getIdsInIndex(indexName, documentType);
            //searchFunctions.getIdsInIndexFunction().apply(indexName, documentType);

        // Remove the database ids from the index ids
        indexDocumentTagIdList.removeAll(databaseTagIdList);

        // If there are any ids left in the index list they need to be removed
        indexDocumentTagIdList.forEach(id -> indexFunctionsDao.deleteDocumentById(indexName, documentType, id));
            //searchFunctions.getDeleteDocumentByIdFunction().accept(indexName, documentType, id));
    }

    /**
     * A helper method that will validate a list of tags
     *
     * @param tagEntityList the list of tags that will be validated
     *
     * @return true all of the tags are valid in the index
     */
    private boolean indexValidateTagsList(final List<TagEntity> tagEntityList)
    {
        final String indexName = SearchIndexTypeEntity.SearchIndexTypes.TAG.name().toLowerCase();
        final String documentType = configurationHelper.getProperty(ConfigurationValue.ELASTICSEARCH_BDEF_DOCUMENT_TYPE, String.class);

        Predicate<TagEntity> validInIndexPredicate = tagEntity -> {
            // Fetch Join with .size()
            tagEntity.getChildrenTagEntities().size();

            // Convert the tag entity to a JSON string
            final String jsonString = tagHelper.safeObjectMapperWriteValueAsString(tagEntity);

            return this.indexFunctionsDao.isValidDocumentIndex(indexName, documentType, tagEntity.getId().toString(), jsonString);
                //searchFunctions.getIsValidFunction().test(indexName, documentType, tagEntity.getId().toString(), jsonString);
        };

        boolean isValid = true;
        for (TagEntity tagEntity : tagEntityList)
        {
            if (!validInIndexPredicate.test(tagEntity))
            {
                isValid = false;
            }
        }

        return isValid;
    }
}
