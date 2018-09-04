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

import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_UPDATE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.config.DaoSpringModuleConfig;
import org.finra.herd.model.annotation.NamespacePermission;
import org.finra.herd.model.annotation.NamespacePermissions;
import org.finra.herd.model.annotation.PublishNotificationMessages;
import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.AttributeDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatAttributeDefinitionsUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatAttributesUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdl;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlCollectionResponse;
import org.finra.herd.model.api.xml.BusinessObjectFormatDdlRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKeys;
import org.finra.herd.model.api.xml.BusinessObjectFormatParentsUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatRetentionInformationUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatSchemaBackwardsCompatibilityUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectFormatUpdateRequest;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.api.xml.NamespacePermissionEnum;
import org.finra.herd.model.api.xml.Schema;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.CustomDdlEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;
import org.finra.herd.model.jpa.RetentionTypeEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;
import org.finra.herd.service.BusinessObjectFormatService;
import org.finra.herd.service.MessageNotificationEventService;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.AttributeHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.helper.BusinessObjectFormatDaoHelper;
import org.finra.herd.service.helper.BusinessObjectFormatHelper;
import org.finra.herd.service.helper.CustomDdlDaoHelper;
import org.finra.herd.service.helper.DdlGenerator;
import org.finra.herd.service.helper.DdlGeneratorFactory;
import org.finra.herd.service.helper.FileTypeDaoHelper;
import org.finra.herd.service.helper.PartitionKeyGroupDaoHelper;
import org.finra.herd.service.helper.SearchIndexUpdateHelper;

/**
 * The business object format service implementation.
 */
@Service
@Transactional(value = DaoSpringModuleConfig.HERD_TRANSACTION_MANAGER_BEAN_NAME)
public class BusinessObjectFormatServiceImpl implements BusinessObjectFormatService
{
    /**
     * List all schema column data types for which size increase is considered to be an additive schema change.
     */
    public static final Set<String> SCHEMA_COLUMN_DATA_TYPES_WITH_ALLOWED_SIZE_INCREASE =
        Collections.unmodifiableSet(new HashSet<>(Arrays.asList("CHAR", "VARCHAR", "VARCHAR2")));


    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private AttributeHelper attributeHelper;

    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private BusinessObjectFormatDaoHelper businessObjectFormatDaoHelper;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private CustomDdlDaoHelper customDdlDaoHelper;

    @Autowired
    private DdlGeneratorFactory ddlGeneratorFactory;

    @Autowired
    private FileTypeDaoHelper fileTypeDaoHelper;

    @Autowired
    private MessageNotificationEventService messageNotificationEventService;

    @Autowired
    private PartitionKeyGroupDaoHelper partitionKeyGroupDaoHelper;

    @Autowired
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

    @PublishNotificationMessages
    @NamespacePermission(fields = "#request.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectFormat createBusinessObjectFormat(BusinessObjectFormatCreateRequest request)
    {
        // Perform the validation of the request parameters, except for the alternate key.
        validateBusinessObjectFormatCreateRequest(request);

        // Get business object format key from the request.
        BusinessObjectFormatKey businessObjectFormatKey = getBusinessObjectFormatKey(request);

        // Get the business object definition and ensure it exists.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(
            new BusinessObjectDefinitionKey(businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName()));

        // Get business object format file type and ensure it exists.
        FileTypeEntity fileTypeEntity = fileTypeDaoHelper.getFileTypeEntity(request.getBusinessObjectFormatFileType());

        // Get the latest format version for this business format, if it exists.
        BusinessObjectFormatEntity latestVersionBusinessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey);

        // Check if the latest version exists.
        if (latestVersionBusinessObjectFormatEntity != null)
        {
            // Get the latest version business object format model object.
            BusinessObjectFormat latestVersionBusinessObjectFormat =
                businessObjectFormatHelper.createBusinessObjectFormatFromEntity(latestVersionBusinessObjectFormatEntity);

            // If the latest version format schema exists and allowNonBackwardsCompatibleChanges is not true,
            // then perform the additive schema validation and update the latest entity.
            if (latestVersionBusinessObjectFormat.getSchema() != null &&
                BooleanUtils.isNotTrue(latestVersionBusinessObjectFormat.isAllowNonBackwardsCompatibleChanges()))
            {
                validateNewSchemaIsAdditiveToOldSchema(request.getSchema(), latestVersionBusinessObjectFormat.getSchema());
            }

            // Update the latest entity.
            latestVersionBusinessObjectFormatEntity.setLatestVersion(false);
            businessObjectFormatDao.saveAndRefresh(latestVersionBusinessObjectFormatEntity);
        }

        // Create a business object format entity from the request information.
        Integer businessObjectFormatVersion =
            latestVersionBusinessObjectFormatEntity == null ? 0 : latestVersionBusinessObjectFormatEntity.getBusinessObjectFormatVersion() + 1;
        BusinessObjectFormatEntity newBusinessObjectFormatEntity =
            createBusinessObjectFormatEntity(request, businessObjectDefinitionEntity, fileTypeEntity, businessObjectFormatVersion, null);
        //latest version format is the descriptive format for the bdef, update the bdef descriptive format to the newly created one
        if (latestVersionBusinessObjectFormatEntity != null &&
            latestVersionBusinessObjectFormatEntity.equals(businessObjectDefinitionEntity.getDescriptiveBusinessObjectFormat()))
        {
            businessObjectDefinitionEntity.setDescriptiveBusinessObjectFormat(newBusinessObjectFormatEntity);
            businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity);
        }
        // latest version business object exists, update its parents and children
        // latest version business object exists, carry the retention information to the new entity
        if (latestVersionBusinessObjectFormatEntity != null)
        {
            //for each of the previous version's  child, remove the parent link to the previous version and set the parent to the new version
            for (BusinessObjectFormatEntity childFormatEntity : latestVersionBusinessObjectFormatEntity.getBusinessObjectFormatChildren())
            {
                childFormatEntity.getBusinessObjectFormatParents().remove(latestVersionBusinessObjectFormatEntity);
                childFormatEntity.getBusinessObjectFormatParents().add(newBusinessObjectFormatEntity);
                newBusinessObjectFormatEntity.getBusinessObjectFormatChildren().add(childFormatEntity);
                businessObjectFormatDao.saveAndRefresh(childFormatEntity);
            }
            //for each of the previous version's parent, remove the child link to the previous version and set the child link to the new version
            for (BusinessObjectFormatEntity parentFormatEntity : latestVersionBusinessObjectFormatEntity.getBusinessObjectFormatParents())
            {
                parentFormatEntity.getBusinessObjectFormatChildren().remove(latestVersionBusinessObjectFormatEntity);
                parentFormatEntity.getBusinessObjectFormatChildren().add(newBusinessObjectFormatEntity);
                newBusinessObjectFormatEntity.getBusinessObjectFormatParents().add(parentFormatEntity);
                businessObjectFormatDao.saveAndRefresh(parentFormatEntity);
            }

            //mark the latest version business object format
            latestVersionBusinessObjectFormatEntity.setBusinessObjectFormatParents(null);
            latestVersionBusinessObjectFormatEntity.setBusinessObjectFormatChildren(null);

            //carry the retention information from the latest entity to the new entity
            newBusinessObjectFormatEntity.setRetentionPeriodInDays(latestVersionBusinessObjectFormatEntity.getRetentionPeriodInDays());
            newBusinessObjectFormatEntity.setRecordFlag(latestVersionBusinessObjectFormatEntity.isRecordFlag());
            newBusinessObjectFormatEntity.setRetentionType(latestVersionBusinessObjectFormatEntity.getRetentionType());

            // Carry the schema backwards compatibility changes from the latest entity to the new entity.
            newBusinessObjectFormatEntity.setAllowNonBackwardsCompatibleChanges(latestVersionBusinessObjectFormatEntity.isAllowNonBackwardsCompatibleChanges());

            businessObjectFormatDao.saveAndRefresh(newBusinessObjectFormatEntity);

            //reset the retention information of the latest version business object format.
            latestVersionBusinessObjectFormatEntity.setRetentionType(null);
            latestVersionBusinessObjectFormatEntity.setRecordFlag(null);
            latestVersionBusinessObjectFormatEntity.setRetentionPeriodInDays(null);

            // Reset the schema backwards compatibility changes of the latest version business object format.
            latestVersionBusinessObjectFormatEntity.setAllowNonBackwardsCompatibleChanges(null);

            businessObjectFormatDao.saveAndRefresh(latestVersionBusinessObjectFormatEntity);
        }

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Create a version change notification to be sent on create business object format event.
        messageNotificationEventService
            .processBusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatHelper.getBusinessObjectFormatKey(newBusinessObjectFormatEntity),
                latestVersionBusinessObjectFormatEntity != null ? latestVersionBusinessObjectFormatEntity.getBusinessObjectFormatVersion().toString() : "");

        // Create and return the business object format object from the persisted entity.
        return businessObjectFormatHelper.createBusinessObjectFormatFromEntity(newBusinessObjectFormatEntity);
    }

    @PublishNotificationMessages
    @NamespacePermission(fields = "#businessObjectFormatKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectFormat updateBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey, BusinessObjectFormatUpdateRequest request)
    {
        // Perform validation and trim the alternate key parameters.
        businessObjectFormatHelper.validateBusinessObjectFormatKey(businessObjectFormatKey);

        // Validate optional attributes. This is also going to trim the attribute names.
        attributeHelper.validateFormatAttributes(request.getAttributes());


        // Retrieve and ensure that a business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);

        // Update business object format description.
        businessObjectFormatEntity.setDescription(request.getDescription());

        // Update business object format document schema
        businessObjectFormatEntity.setDocumentSchema(getTrimmedDocumentSchema(request.getDocumentSchema()));

        // Validate optional schema information.  This is also going to trim the relative schema column field values.
        validateBusinessObjectFormatSchema(request.getSchema(), businessObjectFormatEntity.getPartitionKey());
        // Update business object format attributes
        updateBusinessObjectFormatAttributesHelper(businessObjectFormatEntity, request.getAttributes());

        // Get business object format model object.
        BusinessObjectFormat businessObjectFormat = businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);

        // Check if we need to update business object format schema information.
        if ((request.getSchema() != null && !request.getSchema().equals(businessObjectFormat.getSchema())) ||
            (request.getSchema() == null && businessObjectFormat.getSchema() != null))
        {
            // TODO: Check if we are allowed to update schema information for this business object format.
            //if (businessObjectFormat.getSchema() != null && herdDao.getBusinessObjectDataCount(businessObjectFormatKey) > 0L)
            //{
            //    throw new IllegalArgumentException(String
            //        .format("Can not update schema information for a business object format that has an existing schema " +
            //            "defined and business object data associated with it. Business object format: {%s}",
            //            herdDaoHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
            //}

            // Update schema information by clearing and setting the relative business object
            // format entity fields and by re-creating schema column entities.  Please note that
            // this approach results in changing all schema column Id's which could have
            // ramifications down the road if other entities have relations to schema columns.
            // Also, performance will be slightly slower doing a bunch of deletes followed by a bunch
            // of inserts for what could otherwise be a single SQL statement if only one column was changed.
            // Nevertheless, the below approach results in a simpler code.

            // Removes business object format schema information from the business object format entity.
            clearBusinessObjectFormatSchema(businessObjectFormatEntity);

            // In order to avoid INSERT-then-DELETE, we need to flush the session before we add new schema column entities.
            businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);

            // Populates schema information within the business object format entity.
            populateBusinessObjectFormatSchema(businessObjectFormatEntity, request.getSchema());
        }

        // Persist and refresh the entity.
        businessObjectFormatEntity = businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper
            .modifyBusinessObjectDefinitionInSearchIndex(businessObjectFormatEntity.getBusinessObjectDefinition(), SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Create a version change notification to be sent on create business object format event.
        messageNotificationEventService
            .processBusinessObjectFormatVersionChangeNotificationEvent(businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormatEntity),
                businessObjectFormatEntity.getBusinessObjectFormatVersion().toString());

        // Create and return the business object format object from the persisted entity.
        return businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectFormat getBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey)
    {
        return getBusinessObjectFormatImpl(businessObjectFormatKey);
    }

    /**
     * Gets a business object format for the specified key.
     *
     * @param businessObjectFormatKey the business object format key
     *
     * @return the business object format
     */
    protected BusinessObjectFormat getBusinessObjectFormatImpl(BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Perform validation and trim the alternate key parameters.
        businessObjectFormatHelper.validateBusinessObjectFormatKey(businessObjectFormatKey, false);

        // Retrieve and ensure that a business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);

        boolean checkLatestVersion = false;
        //need to check latest version if the format key does not have the version
        if (businessObjectFormatKey.getBusinessObjectFormatVersion() != null)
        {
            checkLatestVersion = true;
        }

        // Create and return the business object format object from the persisted entity.
        return businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity, checkLatestVersion);
    }

    @NamespacePermission(fields = "#businessObjectFormatKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectFormat deleteBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Perform validation and trim the alternate key parameters.
        businessObjectFormatHelper.validateBusinessObjectFormatKey(businessObjectFormatKey);

        // Retrieve and ensure that a business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);

        // Get the associated Business Object Definition so we can update the search index
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = businessObjectFormatEntity.getBusinessObjectDefinition();

        // Check if we are allowed to delete this business object format.
        if (businessObjectDataDao.getBusinessObjectDataCount(businessObjectFormatKey) > 0L)
        {
            throw new IllegalArgumentException(String
                .format("Can not delete a business object format that has business object data associated with it. Business object format: {%s}",
                    businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
        }

        if (!businessObjectFormatEntity.getBusinessObjectFormatChildren().isEmpty())
        {
            throw new IllegalArgumentException(String
                .format("Can not delete a business object format that has children associated with it. Business object format: {%s}",
                    businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
        }

        // Create and return the business object format object from the deleted entity.
        BusinessObjectFormat deletedBusinessObjectFormat = businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);

        // Check if business object format being deleted is used as a descriptive format.
        if (businessObjectFormatEntity.equals(businessObjectFormatEntity.getBusinessObjectDefinition().getDescriptiveBusinessObjectFormat()))
        {
            businessObjectFormatEntity.getBusinessObjectDefinition().setDescriptiveBusinessObjectFormat(null);
            businessObjectDefinitionDao.saveAndRefresh(businessObjectFormatEntity.getBusinessObjectDefinition());
        }

        // Delete this business object format.
        businessObjectFormatDao.delete(businessObjectFormatEntity);
        // If this business object format version is the latest, set the latest flag on the previous version of this object format, if it exists.
        if (businessObjectFormatEntity.getLatestVersion())
        {
            // Get the maximum version for this business object format, if it exists.
            Integer maxBusinessObjectFormatVersion = businessObjectFormatDao.getBusinessObjectFormatMaxVersion(businessObjectFormatKey);

            if (maxBusinessObjectFormatVersion != null)
            {
                // Retrieve the previous version business object format entity. Since we successfully got the maximum
                // version for this business object format, the retrieved entity is not expected to be null.
                BusinessObjectFormatEntity previousVersionBusinessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
                    new BusinessObjectFormatKey(businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(),
                        businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType(),
                        maxBusinessObjectFormatVersion));

                // Update the previous version business object format entity.
                previousVersionBusinessObjectFormatEntity.setLatestVersion(true);

                // Update the previous version retention information
                previousVersionBusinessObjectFormatEntity.setRecordFlag(businessObjectFormatEntity.isRecordFlag());
                previousVersionBusinessObjectFormatEntity.setRetentionPeriodInDays(businessObjectFormatEntity.getRetentionPeriodInDays());
                previousVersionBusinessObjectFormatEntity.setRetentionType(businessObjectFormatEntity.getRetentionType());

                // Update the previous version schema compatibility changes information.
                previousVersionBusinessObjectFormatEntity
                    .setAllowNonBackwardsCompatibleChanges(businessObjectFormatEntity.isAllowNonBackwardsCompatibleChanges());

                // Save the updated entity.
                businessObjectFormatDao.saveAndRefresh(previousVersionBusinessObjectFormatEntity);
            }
        }

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        return deletedBusinessObjectFormat;
    }

    @Override
    public BusinessObjectFormatKeys getBusinessObjectFormats(BusinessObjectDefinitionKey businessObjectDefinitionKey, boolean latestBusinessObjectFormatVersion)
    {
        // Perform validation and trim.
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);

        // Ensure that a business object definition already exists with the specified name.
        businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Gets the list of keys and return them.
        BusinessObjectFormatKeys businessObjectFormatKeys = new BusinessObjectFormatKeys();
        businessObjectFormatKeys.getBusinessObjectFormatKeys()
            .addAll(businessObjectFormatDao.getBusinessObjectFormats(businessObjectDefinitionKey, latestBusinessObjectFormatVersion));
        return businessObjectFormatKeys;
    }

    @Override
    public BusinessObjectFormatKeys getBusinessObjectFormatsWithFilters(BusinessObjectDefinitionKey businessObjectDefinitionKey,
        String businessObjectFormatUsage, boolean latestBusinessObjectFormatVersion)
    {
        // Perform validation and trim.
        businessObjectDefinitionHelper.validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);

        // Perform Validation and trim of businessObjectFormatUsage
        businessObjectFormatUsage = alternateKeyHelper.validateStringParameter("business object format usage", businessObjectFormatUsage);

        // Ensure that a business object definition already exists with the specified name.
        businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);

        // Gets the list of keys and return them.
        BusinessObjectFormatKeys businessObjectFormatKeys = new BusinessObjectFormatKeys();
        businessObjectFormatKeys.getBusinessObjectFormatKeys().addAll(businessObjectFormatDao
            .getBusinessObjectFormatsWithFilters(businessObjectDefinitionKey, businessObjectFormatUsage, latestBusinessObjectFormatVersion));
        return businessObjectFormatKeys;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectFormatDdl generateBusinessObjectFormatDdl(BusinessObjectFormatDdlRequest request)
    {
        return generateBusinessObjectFormatDdlImpl(request, false);
    }

    @NamespacePermission(fields = "#request?.businessObjectFormatDdlRequests?.![namespace]", permissions = NamespacePermissionEnum.READ)
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public BusinessObjectFormatDdlCollectionResponse generateBusinessObjectFormatDdlCollection(BusinessObjectFormatDdlCollectionRequest request)
    {
        return generateBusinessObjectFormatDdlCollectionImpl(request);
    }

    @NamespacePermissions({@NamespacePermission(fields = "#businessObjectFormatKey.namespace", permissions = NamespacePermissionEnum.WRITE),
        @NamespacePermission(fields = "#businessObjectFormatParentsUpdateRequest?.businessObjectFormatParents?.![namespace]",
            permissions = NamespacePermissionEnum.READ)})
    @Override
    public BusinessObjectFormat updateBusinessObjectFormatParents(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatParentsUpdateRequest businessObjectFormatParentsUpdateRequest)
    {
        Assert.notNull(businessObjectFormatParentsUpdateRequest, "A Business Object Format Parents Update Request is required.");

        // Perform validation and trim the alternate key parameters.
        businessObjectFormatHelper.validateBusinessObjectFormatKey(businessObjectFormatKey, false);

        Assert.isNull(businessObjectFormatKey.getBusinessObjectFormatVersion(), "Business object format version must not be specified.");
        // Perform validation and trim for the business object format parents
        validateBusinessObjectFormatParents(businessObjectFormatParentsUpdateRequest.getBusinessObjectFormatParents());

        // Retrieve and ensure that a business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);

        // Retrieve and ensure that business object format parents exist. A set is used to ignore duplicate business object format parents.
        Set<BusinessObjectFormatEntity> businessObjectFormatParents = new HashSet<>();
        for (BusinessObjectFormatKey businessObjectFormatParent : businessObjectFormatParentsUpdateRequest.getBusinessObjectFormatParents())
        {
            // Retrieve and ensure that a business object format exists.
            BusinessObjectFormatEntity businessObjectFormatEntityParent =
                businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatParent);
            businessObjectFormatParents.add(businessObjectFormatEntityParent);
        }

        // Set the business object format parents.
        businessObjectFormatEntity.setBusinessObjectFormatParents(new ArrayList<>(businessObjectFormatParents));

        // Persist and refresh the entity.
        businessObjectFormatEntity = businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);

        // Notify the search index that a business object definition must be updated.
        searchIndexUpdateHelper
            .modifyBusinessObjectDefinitionInSearchIndex(businessObjectFormatEntity.getBusinessObjectDefinition(), SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        // Create and return the business object format object from the persisted entity.
        return businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
    }

    @NamespacePermission(fields = "#businessObjectFormatKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectFormat updateBusinessObjectFormatAttributes(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatAttributesUpdateRequest businessObjectFormatAttributesUpdateRequest)
    {
        // Perform validation and trim the alternate key parameters.
        businessObjectFormatHelper.validateBusinessObjectFormatKey(businessObjectFormatKey);

        Assert.notNull(businessObjectFormatAttributesUpdateRequest, "A business object format attributes update request is required.");
        Assert.notNull(businessObjectFormatAttributesUpdateRequest.getAttributes(), "A business object format attributes list is required.");
        List<Attribute> attributes = businessObjectFormatAttributesUpdateRequest.getAttributes();
        // Validate optional attributes. This is also going to trim the attribute names.
        attributeHelper.validateFormatAttributes(attributes);

        // Retrieve and ensure that a business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);
        // Update the business object format attributes
        updateBusinessObjectFormatAttributesHelper(businessObjectFormatEntity, attributes);

        // Persist and refresh the entity.
        businessObjectFormatEntity = businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);

        // Create and return the business object format object from the persisted entity.
        return businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
    }

    @NamespacePermission(fields = "#businessObjectFormatKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectFormat updateBusinessObjectFormatAttributeDefinitions(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatAttributeDefinitionsUpdateRequest businessObjectFormatAttributeDefinitionsUpdateRequest)
    {
        // Perform validation and trim the alternate key parameters.
        businessObjectFormatHelper.validateBusinessObjectFormatKey(businessObjectFormatKey);

        Assert.notNull(businessObjectFormatAttributeDefinitionsUpdateRequest, "A business object format attribute definitions update request is required.");

        List<AttributeDefinition> attributeDefinitions = businessObjectFormatAttributeDefinitionsUpdateRequest.getAttributeDefinitions();
        // Validate and trim optional attribute definitions. This is also going to trim the attribute definition names.
        validateAndTrimBusinessObjectFormatAttributeDefinitionsHelper(attributeDefinitions);

        // Retrieve and ensure that a business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);
        // Update the business object format attributes
        updateBusinessObjectFormatAttributeDefinitionsHelper(businessObjectFormatEntity, attributeDefinitions);

        // Persist and refresh the entity.
        businessObjectFormatEntity = businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);

        // Create and return the business object format object from the persisted entity.
        return businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system (e.g. Hive) by creating a table for the requested business object format.
     *
     * @param request the business object format DDL request
     * @param skipRequestValidation specifies whether to skip the request validation and trimming
     *
     * @return the business object format DDL information
     */
    protected BusinessObjectFormatDdl generateBusinessObjectFormatDdlImpl(BusinessObjectFormatDdlRequest request, boolean skipRequestValidation)
    {
        // Perform the validation.
        if (!skipRequestValidation)
        {
            validateBusinessObjectFormatDdlRequest(request);
        }

        // Get the business object format entity for the specified parameters and make sure it exists.
        // Please note that when format version is not specified, we should get back the latest format version.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(
            new BusinessObjectFormatKey(request.getNamespace(), request.getBusinessObjectDefinitionName(), request.getBusinessObjectFormatUsage(),
                request.getBusinessObjectFormatFileType(), request.getBusinessObjectFormatVersion()));

        // Get business object format key.
        BusinessObjectFormatKey businessObjectFormatKey = businessObjectFormatHelper.getBusinessObjectFormatKey(businessObjectFormatEntity);

        // Validate that format has schema information.
        Assert.notEmpty(businessObjectFormatEntity.getSchemaColumns(), String.format(
            "Business object format with namespace \"%s\", business object definition name \"%s\", format usage \"%s\", format file type \"%s\"," +
                " and format version \"%s\" doesn't have schema information.", businessObjectFormatKey.getNamespace(),
            businessObjectFormatKey.getBusinessObjectDefinitionName(), businessObjectFormatKey.getBusinessObjectFormatUsage(),
            businessObjectFormatKey.getBusinessObjectFormatFileType(), businessObjectFormatKey.getBusinessObjectFormatVersion()));

        // If it was specified, retrieve the custom DDL and ensure it exists.
        CustomDdlEntity customDdlEntity = null;
        if (StringUtils.isNotBlank(request.getCustomDdlName()))
        {
            customDdlEntity = customDdlDaoHelper.getCustomDdlEntity(
                new CustomDdlKey(businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(),
                    businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType(),
                    businessObjectFormatKey.getBusinessObjectFormatVersion(), request.getCustomDdlName()));
        }

        // Create business object format DDL object instance.
        BusinessObjectFormatDdl businessObjectFormatDdl = new BusinessObjectFormatDdl();
        businessObjectFormatDdl.setNamespace(businessObjectFormatKey.getNamespace());
        businessObjectFormatDdl.setBusinessObjectDefinitionName(businessObjectFormatKey.getBusinessObjectDefinitionName());
        businessObjectFormatDdl.setBusinessObjectFormatUsage(businessObjectFormatKey.getBusinessObjectFormatUsage());
        businessObjectFormatDdl.setBusinessObjectFormatFileType(businessObjectFormatKey.getBusinessObjectFormatFileType());
        businessObjectFormatDdl.setBusinessObjectFormatVersion(businessObjectFormatKey.getBusinessObjectFormatVersion());
        businessObjectFormatDdl.setOutputFormat(request.getOutputFormat());
        businessObjectFormatDdl.setTableName(request.getTableName());
        businessObjectFormatDdl.setCustomDdlName(customDdlEntity != null ? customDdlEntity.getCustomDdlName() : request.getCustomDdlName());
        DdlGenerator ddlGenerator = ddlGeneratorFactory.getDdlGenerator(request.getOutputFormat());
        String ddl;
        if (Boolean.TRUE.equals(request.isReplaceColumns()))
        {
            ddl = ddlGenerator.generateReplaceColumnsStatement(request, businessObjectFormatEntity);
        }
        else
        {
            ddl = ddlGenerator.generateCreateTableDdl(request, businessObjectFormatEntity, customDdlEntity);
        }
        businessObjectFormatDdl.setDdl(ddl);

        // Return business object format DDL.
        return businessObjectFormatDdl;
    }

    /**
     * Retrieves the DDL to initialize the specified type of the database system (e.g. Hive) by creating tables for a collection of business object formats.
     *
     * @param businessObjectFormatDdlCollectionRequest the business object format DDL collection request
     *
     * @return the business object format DDL information
     */
    protected BusinessObjectFormatDdlCollectionResponse generateBusinessObjectFormatDdlCollectionImpl(
        BusinessObjectFormatDdlCollectionRequest businessObjectFormatDdlCollectionRequest)
    {
        // Perform the validation of the entire request, before we start processing the individual requests that requires the database access.
        validateBusinessObjectFormatDdlCollectionRequest(businessObjectFormatDdlCollectionRequest);

        // Process the individual requests and build the response.
        BusinessObjectFormatDdlCollectionResponse businessObjectFormatDdlCollectionResponse = new BusinessObjectFormatDdlCollectionResponse();
        List<BusinessObjectFormatDdl> businessObjectFormatDdlResponses = new ArrayList<>();
        businessObjectFormatDdlCollectionResponse.setBusinessObjectFormatDdlResponses(businessObjectFormatDdlResponses);
        List<String> ddls = new ArrayList<>();
        for (BusinessObjectFormatDdlRequest request : businessObjectFormatDdlCollectionRequest.getBusinessObjectFormatDdlRequests())
        {
            // Please note that when calling to process individual ddl requests, we ask to skip the request validation and trimming step.
            BusinessObjectFormatDdl businessObjectFormatDdl = generateBusinessObjectFormatDdlImpl(request, true);
            businessObjectFormatDdlResponses.add(businessObjectFormatDdl);
            ddls.add(businessObjectFormatDdl.getDdl());
        }
        businessObjectFormatDdlCollectionResponse.setDdlCollection(StringUtils.join(ddls, "\n\n"));

        return businessObjectFormatDdlCollectionResponse;
    }

    @NamespacePermission(fields = "#businessObjectFormatKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectFormat updateBusinessObjectFormatRetentionInformation(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatRetentionInformationUpdateRequest updateRequest)
    {
        // Validate and trim the business object format retention information update request update request.
        Assert.notNull(updateRequest, "A business object format retention information update request must be specified.");
        Assert.notNull(updateRequest.isRecordFlag(), "A record flag in business object format retention information update request must be specified.");
        businessObjectFormatHelper.validateBusinessObjectFormatKey(businessObjectFormatKey, false);
        Assert.isNull(businessObjectFormatKey.getBusinessObjectFormatVersion(), "Business object format version must not be specified.");

        // Validate business object format retention information.
        RetentionTypeEntity recordRetentionTypeEntity = null;
        if (updateRequest.getRetentionType() != null)
        {
            // Trim the business object format retention in update request.
            updateRequest.setRetentionType(updateRequest.getRetentionType().trim());

            // Retrieve and ensure that a retention type exists.
            recordRetentionTypeEntity = businessObjectFormatDaoHelper.getRecordRetentionTypeEntity(updateRequest.getRetentionType());

            if (recordRetentionTypeEntity.getCode().equals(RetentionTypeEntity.PARTITION_VALUE))
            {
                Assert.notNull(updateRequest.getRetentionPeriodInDays(),
                    String.format("A retention period in days must be specified for %s retention type.", RetentionTypeEntity.PARTITION_VALUE));
                Assert.isTrue(updateRequest.getRetentionPeriodInDays() > 0,
                    String.format("A positive retention period in days must be specified for %s retention type.", RetentionTypeEntity.PARTITION_VALUE));
            }
            else
            {
                Assert.isNull(updateRequest.getRetentionPeriodInDays(),
                    String.format("A retention period in days cannot be specified for %s retention type.", recordRetentionTypeEntity.getCode()));
            }
        }
        else
        {
            // Do not allow retention period to be specified without retention type.
            Assert.isNull(updateRequest.getRetentionPeriodInDays(), "A retention period in days cannot be specified without retention type.");
        }

        // Retrieve and ensure that a business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);
        businessObjectFormatEntity.setRecordFlag(BooleanUtils.isTrue(updateRequest.isRecordFlag()));
        businessObjectFormatEntity.setRetentionPeriodInDays(updateRequest.getRetentionPeriodInDays());
        businessObjectFormatEntity.setRetentionType(recordRetentionTypeEntity);

        // Persist and refresh the entity.
        businessObjectFormatEntity = businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);

        // Create and return the business object format object from the persisted entity.
        return businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
    }

    @NamespacePermission(fields = "#businessObjectFormatKey.namespace", permissions = NamespacePermissionEnum.WRITE)
    @Override
    public BusinessObjectFormat updateBusinessObjectFormatSchemaBackwardsCompatibilityChanges(BusinessObjectFormatKey businessObjectFormatKey,
        BusinessObjectFormatSchemaBackwardsCompatibilityUpdateRequest businessObjectFormatSchemaBackwardsCompatibilityUpdateRequest)
    {
        // Validate business object format schema backwards compatibility changes update request.
        Assert.notNull(businessObjectFormatSchemaBackwardsCompatibilityUpdateRequest,
            "A business object format schema backwards compatibility changes update request must be specified.");
        Assert.notNull(businessObjectFormatSchemaBackwardsCompatibilityUpdateRequest.isAllowNonBackwardsCompatibleChanges(),
            "allowNonBackwardsCompatibleChanges flag in business object format schema backwards compatibility changes update request must be specified.");

        // Validate and trim the business object format key parameters.
        businessObjectFormatHelper.validateBusinessObjectFormatKey(businessObjectFormatKey, false);
        Assert.isNull(businessObjectFormatKey.getBusinessObjectFormatVersion(), "Business object format version must not be specified.");

        // Retrieve and ensure that a business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDaoHelper.getBusinessObjectFormatEntity(businessObjectFormatKey);
        businessObjectFormatEntity.setAllowNonBackwardsCompatibleChanges(
            BooleanUtils.isTrue(businessObjectFormatSchemaBackwardsCompatibilityUpdateRequest.isAllowNonBackwardsCompatibleChanges()));

        // Persist and refresh the entity.
        businessObjectFormatEntity = businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);

        // Create and return the business object format object from the persisted entity.
        return businessObjectFormatHelper.createBusinessObjectFormatFromEntity(businessObjectFormatEntity);
    }

    /**
     * Validates the business object format create request, except for the alternate key values. This method also trims request parameters.
     *
     * @param request the business object format create request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectFormatCreateRequest(BusinessObjectFormatCreateRequest request)
    {
        // Extract business object format key from the create request.
        BusinessObjectFormatKey businessObjectFormatKey = getBusinessObjectFormatKey(request);

        // Perform validation and trim the business object format key parameters, except for a business object format version.
        businessObjectFormatHelper.validateBusinessObjectFormatKey(businessObjectFormatKey, false);

        // Update the request object instance with the alternate key parameters.
        updateBusinessObjectFormatAlternateKeyOnCreateRequest(request, businessObjectFormatKey);

        // Perform validation of the partition key. This method also trims the partition key value.
        request.setPartitionKey(alternateKeyHelper.validateStringParameter("partition key", request.getPartitionKey()));

        // Validate attributes.
        attributeHelper.validateFormatAttributes(request.getAttributes());

        // Validate attribute definitions if they are specified.
        if (!CollectionUtils.isEmpty(request.getAttributeDefinitions()))
        {
            Map<String, AttributeDefinition> attributeNameValidationMap = new HashMap<>();
            for (AttributeDefinition attributeDefinition : request.getAttributeDefinitions())
            {
                Assert.hasText(attributeDefinition.getName(), "An attribute definition name must be specified.");
                attributeDefinition.setName(attributeDefinition.getName().trim());

                // Ensure the attribute key isn't a duplicate by using a map with a "lowercase" name as the key for case insensitivity.
                String lowercaseAttributeDefinitionName = attributeDefinition.getName().toLowerCase();
                if (attributeNameValidationMap.containsKey(lowercaseAttributeDefinitionName))
                {
                    throw new IllegalArgumentException(String.format("Duplicate attribute definition name \"%s\" found.", attributeDefinition.getName()));
                }
                attributeNameValidationMap.put(lowercaseAttributeDefinitionName, attributeDefinition);
            }
        }

        // Validate optional schema information.
        validateBusinessObjectFormatSchema(request.getSchema(), request.getPartitionKey());
    }

    /**
     * Validate the business object format parents
     *
     * @param businessObjectFormatParents business object format parents
     */
    private void validateBusinessObjectFormatParents(List<BusinessObjectFormatKey> businessObjectFormatParents)
    {
        // Validate parents business object format.
        if (!CollectionUtils.isEmpty(businessObjectFormatParents))
        {
            for (BusinessObjectFormatKey parentBusinessObjectFormatKey : businessObjectFormatParents)
            {
                Assert.notNull(parentBusinessObjectFormatKey, "Parent object format must be specified.");
                businessObjectFormatHelper.validateBusinessObjectFormatKey(parentBusinessObjectFormatKey, false);
                Assert.isNull(parentBusinessObjectFormatKey.getBusinessObjectFormatVersion(), "Business object format version must not be specified.");
            }
        }
    }

    /**
     * Validates the business object format schema. This method also trims some of the schema column attributes parameters.
     *
     * @param schema the business object format schema
     * @param partitionKey the business object format partition key
     *
     * @throws IllegalArgumentException if any validation errors were found.
     */
    private void validateBusinessObjectFormatSchema(Schema schema, String partitionKey)
    {
        // Validate optional schema information.
        if (schema != null)
        {
            // Perform validation.
            Assert.notNull(schema.getNullValue(), "A schema null value can not be null.");

            // Remove leading and trailing spaces.
            // Note that we don't trim the row option characters since they could contain white space or non-ASCII characters which would get removed if
            // "trim"ed.
            schema.setPartitionKeyGroup(schema.getPartitionKeyGroup() == null ? null : schema.getPartitionKeyGroup().trim());

            Assert.isTrue(!CollectionUtils.isEmpty(schema.getColumns()), "A schema must have at least one column.");

            // Validates the schema columns. We are creating a map here since it is used across both data and partition columns.
            LinkedHashMap<String, SchemaColumn> schemaEqualityValidationMap = new LinkedHashMap<>();
            validateSchemaColumns(schema.getColumns(), schemaEqualityValidationMap);
            validateSchemaColumns(schema.getPartitions(), schemaEqualityValidationMap);

            // Ensure the partition key matches the first schema column name if a single partition column is present.
            // TODO: We are doing this check since the primary partition is specified in 2 places and we want to keep them in sync. In the future,
            // the partition key will go away and this check will be removed.
            if (!CollectionUtils.isEmpty(schema.getPartitions()))
            {
                SchemaColumn schemaColumn = schema.getPartitions().get(0);
                if (!partitionKey.equalsIgnoreCase(schemaColumn.getName()))
                {
                    throw new IllegalArgumentException(String
                        .format("Partition key \"%s\" does not match the first schema partition column name \"%s\".", partitionKey, schemaColumn.getName()));
                }
            }
        }
    }

    /**
     * Validates a list of schema columns.
     *
     * @param schemaColumns the list of schema columns.
     * @param schemaEqualityValidationMap a map of schema column names to their schema column. This is used to check equality across all data schema columns as
     * well as partition schema columns.
     */
    private void validateSchemaColumns(List<SchemaColumn> schemaColumns, LinkedHashMap<String, SchemaColumn> schemaEqualityValidationMap)
    {
        // Validate schema columns if they are specified.
        if (!CollectionUtils.isEmpty(schemaColumns))
        {
            // Create a schema column name map that we will use to check for duplicate
            // columns for the specified list of schema columns (i.e. data or partition).
            LinkedHashMap<String, SchemaColumn> schemaColumnNameValidationMap = new LinkedHashMap<>();

            // Loop through each schema column in the list.
            for (SchemaColumn schemaColumn : schemaColumns)
            {
                // Perform validation.
                Assert.hasText(schemaColumn.getName(), "A schema column name must be specified.");
                Assert.hasText(schemaColumn.getType(), "A schema column data type must be specified.");

                // Remove leading and trailing spaces.
                schemaColumn.setName(schemaColumn.getName().trim());
                schemaColumn.setType(schemaColumn.getType().trim());
                schemaColumn.setSize(schemaColumn.getSize() == null ? null : schemaColumn.getSize().trim());
                schemaColumn.setDefaultValue(schemaColumn.getDefaultValue() == null ? null : schemaColumn.getDefaultValue().trim());

                // Ensure the column name isn't a duplicate within this list only by using a map.
                String lowercaseSchemaColumnName = schemaColumn.getName().toLowerCase();
                if (schemaColumnNameValidationMap.containsKey(lowercaseSchemaColumnName))
                {
                    throw new IllegalArgumentException(String.format("Duplicate schema column name \"%s\" found.", schemaColumn.getName()));
                }
                schemaColumnNameValidationMap.put(lowercaseSchemaColumnName, schemaColumn);

                // Ensure a partition column and a data column are equal (i.e. contain the same configuration).
                SchemaColumn existingSchemaColumn = schemaEqualityValidationMap.get(lowercaseSchemaColumnName);
                if ((existingSchemaColumn != null) && !schemaColumn.equals(existingSchemaColumn))
                {
                    throw new IllegalArgumentException("Schema data and partition column configurations with name \"" + schemaColumn.getName() +
                        "\" have conflicting values. All column values are case sensitive and must be identical.");
                }
                schemaEqualityValidationMap.put(lowercaseSchemaColumnName, schemaColumn);
            }
        }
    }

    /**
     * Validates that the new business object format version schema is additive to the old one.
     *
     * @param newSchema the new schema
     * @param oldSchema the old schema
     */
    private void validateNewSchemaIsAdditiveToOldSchema(Schema newSchema, Schema oldSchema)
    {
        String mainErrorMessage = "New format version schema is not \"additive\" to the previous format version schema.";

        // Validate that new version format schema is specified.
        Assert.notNull(newSchema, String.format("%s New format version schema is not specified.", mainErrorMessage));

        // Validate that there are no changes to the schema null value, which is a required parameter.
        // Please note that NULL value in the database represents an empty string.
        Assert.isTrue(oldSchema.getNullValue() == null ? newSchema.getNullValue().isEmpty() : oldSchema.getNullValue().equals(newSchema.getNullValue()),
            String.format("%s New format version null value does not match to the previous format version null value.", mainErrorMessage));

        // Validate that there are no changes to the delimiter character, which is a an optional parameter.
        // Please note that null and an empty string values are both stored in the database as NULL.
        Assert.isTrue(oldSchema.getDelimiter() == null ? newSchema.getDelimiter() == null || newSchema.getDelimiter().isEmpty() :
                oldSchema.getDelimiter().equals(newSchema.getDelimiter()),
            String.format("%s New format version delimiter character does not match to the previous format version delimiter character.", mainErrorMessage));

        // Validate that there are no changes to the escape character, which is a an optional parameter.
        // Please note that null and an empty string values are both stored in the database as NULL.
        Assert.isTrue(oldSchema.getEscapeCharacter() == null ? newSchema.getEscapeCharacter() == null || newSchema.getEscapeCharacter().isEmpty() :
                oldSchema.getEscapeCharacter().equals(newSchema.getEscapeCharacter()),
            String.format("%s New format version escape character does not match to the previous format version escape character.", mainErrorMessage));

        // Validate that there are no non-additive changes to partition columns.
        Assert.isTrue(validateNewSchemaColumnsAreAdditiveToOldSchemaColumns(newSchema.getPartitions(), oldSchema.getPartitions()),
            String.format("%s Non-additive changes detected to the previously defined partition columns.", mainErrorMessage));

        // Validate that there are no non-additive changes to the previous format version regular columns.
        // No null check is needed on schema columns, since at least one column is required if format schema is specified.
        Assert.isTrue((oldSchema.getColumns().size() <= newSchema.getColumns().size()) &&
                validateNewSchemaColumnsAreAdditiveToOldSchemaColumns(newSchema.getColumns().subList(0, oldSchema.getColumns().size()), oldSchema.getColumns()),
            String.format("%s Non-additive changes detected to the previously defined regular (non-partitioning) columns.", mainErrorMessage));
    }

    /**
     * Validate that a new list of schema columns is additive to an old one.
     *
     * @param newSchemaColumns the list of schema columns from the new schema
     * @param oldSchemaColumns the list of schema columns from the old schema
     *
     * @return true if schema columns are additive, false otherwise
     */
    private boolean validateNewSchemaColumnsAreAdditiveToOldSchemaColumns(List<SchemaColumn> newSchemaColumns, List<SchemaColumn> oldSchemaColumns)
    {
        // Null check is required for the new and old list of columns, since partition columns are optional in format schema.
        if (newSchemaColumns == null || oldSchemaColumns == null)
        {
            return (newSchemaColumns == null && oldSchemaColumns == null);
        }
        else if (newSchemaColumns.size() != oldSchemaColumns.size())
        {
            return false;
        }
        else
        {
            // Loop through the schema columns and compare each pair of new and old columns.
            for (int i = 0; i < newSchemaColumns.size(); i++)
            {
                // Get instance copies for both new and old schema columns.
                // Null check is not needed here, since schema column instance can not be null.
                SchemaColumn newSchemaColumnCopy = (SchemaColumn) newSchemaColumns.get(i).clone();
                SchemaColumn oldSchemaColumnCopy = (SchemaColumn) oldSchemaColumns.get(i).clone();

                // Since we allow column description to be changed, set the description field to null for both columns.
                newSchemaColumnCopy.setDescription(null);
                oldSchemaColumnCopy.setDescription(null);

                // For a set of data types, size increase is considered to be an additive change.
                // Null check on schema column date type is not needed, since it is a required field for schema column.
                if (SCHEMA_COLUMN_DATA_TYPES_WITH_ALLOWED_SIZE_INCREASE.contains(newSchemaColumnCopy.getType().toUpperCase()))
                {
                    // The below string to integer conversion methods return an int represented by the string, or zero if conversion fails.
                    int newSize = NumberUtils.toInt(newSchemaColumnCopy.getSize());
                    int oldSize = NumberUtils.toInt(oldSchemaColumnCopy.getSize());

                    // If new size is greater than the old one and they are both positive integers, then set the size field to null for both columns.
                    if (oldSize > 0 && newSize >= oldSize)
                    {
                        newSchemaColumnCopy.setSize(null);
                        oldSchemaColumnCopy.setSize(null);
                    }
                }

                // Fail the validation if columns do not match.
                if (!newSchemaColumnCopy.equals(oldSchemaColumnCopy))
                {
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * Validates a business object format DDL collection request. This method also trims appropriate request parameters.
     *
     * @param businessObjectFormatDdlCollectionRequest the request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectFormatDdlCollectionRequest(BusinessObjectFormatDdlCollectionRequest businessObjectFormatDdlCollectionRequest)
    {
        Assert.notNull(businessObjectFormatDdlCollectionRequest, "A business object format DDL collection request must be specified.");

        Assert.isTrue(!CollectionUtils.isEmpty(businessObjectFormatDdlCollectionRequest.getBusinessObjectFormatDdlRequests()),
            "At least one business object format DDL request must be specified.");

        for (BusinessObjectFormatDdlRequest request : businessObjectFormatDdlCollectionRequest.getBusinessObjectFormatDdlRequests())
        {
            validateBusinessObjectFormatDdlRequest(request);
        }
    }

    /**
     * Validates the business object format DDL request. This method also trims appropriate request parameters.
     *
     * @param request the business object format DDL request
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    private void validateBusinessObjectFormatDdlRequest(BusinessObjectFormatDdlRequest request)
    {
        Assert.notNull(request, "A business object format DDL request must be specified.");

        // Validate and trim the request parameters.
        Assert.hasText(request.getNamespace(), "A namespace must be specified.");
        request.setNamespace(request.getNamespace().trim());

        Assert.hasText(request.getBusinessObjectDefinitionName(), "A business object definition name must be specified.");
        request.setBusinessObjectDefinitionName(request.getBusinessObjectDefinitionName().trim());

        Assert.hasText(request.getBusinessObjectFormatUsage(), "A business object format usage must be specified.");
        request.setBusinessObjectFormatUsage(request.getBusinessObjectFormatUsage().trim());

        Assert.hasText(request.getBusinessObjectFormatFileType(), "A business object format file type must be specified.");
        request.setBusinessObjectFormatFileType(request.getBusinessObjectFormatFileType().trim());

        Assert.notNull(request.getOutputFormat(), "An output format must be specified.");

        Assert.hasText(request.getTableName(), "A table name must be specified.");
        request.setTableName(request.getTableName().trim());

        if (BooleanUtils.isTrue(request.isReplaceColumns()))
        {
            Assert.isTrue(BooleanUtils.isNotTrue(request.isIncludeDropTableStatement()),
                "'includeDropTableStatement' must not be specified when 'replaceColumns' is true");
            Assert.isTrue(BooleanUtils.isNotTrue(request.isIncludeIfNotExistsOption()),
                "'includeIfNotExistsOption' must not be specified when 'replaceColumns' is true");
            Assert.isTrue(StringUtils.isBlank(request.getCustomDdlName()), "'customDdlName' must not be specified when 'replaceColumns' is true");
        }

        if (request.getCustomDdlName() != null)
        {
            request.setCustomDdlName(request.getCustomDdlName().trim());
        }
    }

    /**
     * Creates a business object format key from the relative values in the business object format create request.  Please note that the key will contain a null
     * for the business object format version value.
     *
     * @param businessObjectFormatCreateRequest the business object format create request
     *
     * @return the business object format key
     */
    private BusinessObjectFormatKey getBusinessObjectFormatKey(BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest)
    {
        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey();

        businessObjectFormatKey.setNamespace(businessObjectFormatCreateRequest.getNamespace());
        businessObjectFormatKey.setBusinessObjectDefinitionName(businessObjectFormatCreateRequest.getBusinessObjectDefinitionName());
        businessObjectFormatKey.setBusinessObjectFormatUsage(businessObjectFormatCreateRequest.getBusinessObjectFormatUsage());
        businessObjectFormatKey.setBusinessObjectFormatFileType(businessObjectFormatCreateRequest.getBusinessObjectFormatFileType());

        return businessObjectFormatKey;
    }

    /**
     * Sets the relative fields in the business object format create request per specified business object format alternate key.
     *
     * @param businessObjectFormatCreateRequest the business object format create request
     * @param businessObjectFormatKey the business object format alternate key
     */
    private void updateBusinessObjectFormatAlternateKeyOnCreateRequest(BusinessObjectFormatCreateRequest businessObjectFormatCreateRequest,
        BusinessObjectFormatKey businessObjectFormatKey)
    {
        businessObjectFormatCreateRequest.setNamespace(businessObjectFormatKey.getNamespace());
        businessObjectFormatCreateRequest.setBusinessObjectDefinitionName(businessObjectFormatKey.getBusinessObjectDefinitionName());
        businessObjectFormatCreateRequest.setBusinessObjectFormatUsage(businessObjectFormatKey.getBusinessObjectFormatUsage());
        businessObjectFormatCreateRequest.setBusinessObjectFormatFileType(businessObjectFormatKey.getBusinessObjectFormatFileType());
    }

    /**
     * Creates and persists a new business object format entity from the request information.
     *
     * @param request the request.
     * @param businessObjectFormatEntityParents parent business object format entity
     *
     * @return the newly created business object format entity.
     */
    private BusinessObjectFormatEntity createBusinessObjectFormatEntity(BusinessObjectFormatCreateRequest request,
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity, FileTypeEntity fileTypeEntity, Integer businessObjectFormatVersion,
        List<BusinessObjectFormatEntity> businessObjectFormatEntityParents)
    {
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setUsage(request.getBusinessObjectFormatUsage());
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectFormatEntity.setLatestVersion(Boolean.TRUE);
        businessObjectFormatEntity.setPartitionKey(request.getPartitionKey());
        businessObjectFormatEntity.setDescription(request.getDescription());
        businessObjectFormatEntity.setDocumentSchema(getTrimmedDocumentSchema(request.getDocumentSchema()));

        // Create the attributes if they are specified.
        if (!CollectionUtils.isEmpty(request.getAttributes()))
        {
            List<BusinessObjectFormatAttributeEntity> attributeEntities = new ArrayList<>();
            businessObjectFormatEntity.setAttributes(attributeEntities);
            for (Attribute attribute : request.getAttributes())
            {
                BusinessObjectFormatAttributeEntity attributeEntity = new BusinessObjectFormatAttributeEntity();
                attributeEntities.add(attributeEntity);
                attributeEntity.setBusinessObjectFormat(businessObjectFormatEntity);
                attributeEntity.setName(attribute.getName());
                attributeEntity.setValue(attribute.getValue());
            }
        }


        businessObjectFormatEntity.setAttributeDefinitions(createAttributeDefinitionEntities(request.getAttributeDefinitions(), businessObjectFormatEntity));

        // Add optional schema information.
        populateBusinessObjectFormatSchema(businessObjectFormatEntity, request.getSchema());

        //set the parents
        businessObjectFormatEntity.setBusinessObjectFormatParents(businessObjectFormatEntityParents);

        // Persist and return the new entity.
        return businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);
    }

    /**
     * Creates a list of attribute definition entities.
     *
     * @param attributeDefinitions the list of attribute definitions
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return the newly created list of attribute definition entities
     */
    private List<BusinessObjectDataAttributeDefinitionEntity> createAttributeDefinitionEntities(List<AttributeDefinition> attributeDefinitions,
        BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        List<BusinessObjectDataAttributeDefinitionEntity> attributeDefinitionEntities = new ArrayList<>();

        if (!CollectionUtils.isEmpty(attributeDefinitions))
        {
            for (AttributeDefinition attributeDefinition : attributeDefinitions)
            {
                // Create a new business object data attribute definition entity.
                BusinessObjectDataAttributeDefinitionEntity attributeDefinitionEntity = new BusinessObjectDataAttributeDefinitionEntity();
                attributeDefinitionEntities.add(attributeDefinitionEntity);
                attributeDefinitionEntity.setBusinessObjectFormat(businessObjectFormatEntity);
                attributeDefinitionEntity.setName(attributeDefinition.getName());

                // For the "publish" option, default a Boolean null value to "false".
                attributeDefinitionEntity.setPublish(BooleanUtils.isTrue(attributeDefinition.isPublish()));
            }
        }

        return attributeDefinitionEntities;
    }

    /**
     * Adds business object schema information to the business object format entity.
     *
     * @param businessObjectFormatEntity the business object format entity
     * @param schema the schema from the business object format create request
     */
    private void populateBusinessObjectFormatSchema(BusinessObjectFormatEntity businessObjectFormatEntity, Schema schema)
    {
        // Add optional schema information.
        if (schema != null)
        {
            // If optional partition key group value is specified, get the partition key group entity and ensure it exists.
            PartitionKeyGroupEntity partitionKeyGroupEntity = null;
            if (StringUtils.isNotBlank(schema.getPartitionKeyGroup()))
            {
                partitionKeyGroupEntity = partitionKeyGroupDaoHelper.getPartitionKeyGroupEntity(schema.getPartitionKeyGroup());
            }

            businessObjectFormatEntity.setNullValue(schema.getNullValue());
            businessObjectFormatEntity.setDelimiter(schema.getDelimiter());
            businessObjectFormatEntity.setEscapeCharacter(schema.getEscapeCharacter());
            businessObjectFormatEntity.setPartitionKeyGroup(partitionKeyGroupEntity);

            // Create a schema column entities collection, if needed.
            Collection<SchemaColumnEntity> schemaColumnEntities = businessObjectFormatEntity.getSchemaColumns();
            if (schemaColumnEntities == null)
            {
                schemaColumnEntities = new ArrayList<>();
                businessObjectFormatEntity.setSchemaColumns(schemaColumnEntities);
            }

            // Create a map that will easily let us keep track of schema column entities we're creating by their column name.
            Map<String, SchemaColumnEntity> schemaColumnMap = new HashMap<>();

            // Create the schema columns for both the data columns and then the partition columns.
            // Since both lists share the same schema column entity list, we will use a common method to process them in the same way.
            createSchemaColumnEntities(schema.getColumns(), false, schemaColumnEntities, schemaColumnMap, businessObjectFormatEntity);
            createSchemaColumnEntities(schema.getPartitions(), true, schemaColumnEntities, schemaColumnMap, businessObjectFormatEntity);
        }
    }

    /**
     * Creates the schema column entities.
     *
     * @param schemaColumns the list of schema columns (for either the data columns or the partition columns).
     * @param isPartitionList A flag to specify whether the list of schema columns is a data column or a partition column list.
     * @param schemaColumnEntityList the list of schema column entities we're creating. This method will add a new one to the list if it hasn't been created
     * before.
     * @param schemaColumnEntityMap a map of schema column entity names to the schema column entity. This is used so we don't have to keep searching the
     * schemaColumnEntityList which is less efficient.
     * @param businessObjectFormatEntity the business object format entity to associated each newly created schema column entity with.
     */
    private void createSchemaColumnEntities(List<SchemaColumn> schemaColumns, boolean isPartitionList, Collection<SchemaColumnEntity> schemaColumnEntityList,
        Map<String, SchemaColumnEntity> schemaColumnEntityMap, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        // Create the relative database entities if schema columns are specified.
        if (!CollectionUtils.isEmpty(schemaColumns))
        {
            // Initialize a position counter since the order we encounter the schema columns is the order we will set on the entity.
            int position = 1;

            // Loop through each schema column in the list.
            for (SchemaColumn schemaColumn : schemaColumns)
            {
                // See if we created the schema column already. If not, create it and add it to the map and the collection.
                SchemaColumnEntity schemaColumnEntity = schemaColumnEntityMap.get(schemaColumn.getName());
                if (schemaColumnEntity == null)
                {
                    schemaColumnEntity = createSchemaColumnEntity(schemaColumn, businessObjectFormatEntity);
                    schemaColumnEntityList.add(schemaColumnEntity);
                    schemaColumnEntityMap.put(schemaColumn.getName(), schemaColumnEntity);
                }

                // Set the position or partition level depending on the type of object we're processing.
                if (isPartitionList)
                {
                    schemaColumnEntity.setPartitionLevel(position++);
                }
                else
                {
                    schemaColumnEntity.setPosition(position++);
                }
            }
        }
    }

    /**
     * Creates a new schema column entity from the schema column.
     *
     * @param schemaColumn the schema column.
     * @param businessObjectFormatEntity the business object format entity to associated each newly created schema column entity with.
     *
     * @return the newly created schema column entity.
     */
    private SchemaColumnEntity createSchemaColumnEntity(SchemaColumn schemaColumn, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();

        schemaColumnEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        schemaColumnEntity.setName(schemaColumn.getName());
        schemaColumnEntity.setType(schemaColumn.getType());
        schemaColumnEntity.setSize(schemaColumn.getSize());
        schemaColumnEntity.setRequired(schemaColumn.isRequired());
        schemaColumnEntity.setDefaultValue(schemaColumn.getDefaultValue());
        schemaColumnEntity.setDescription(schemaColumn.getDescription());

        return schemaColumnEntity;
    }

    /**
     * Deletes business object format schema information for the specified business object format entity.
     *
     * @param businessObjectFormatEntity the business object format entity
     */
    private void clearBusinessObjectFormatSchema(BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        businessObjectFormatEntity.setNullValue(null);
        businessObjectFormatEntity.setDelimiter(null);
        businessObjectFormatEntity.setEscapeCharacter(null);
        businessObjectFormatEntity.setPartitionKeyGroup(null);
        businessObjectFormatEntity.getSchemaColumns().clear();
    }

    /**
     * Updates business object format attributes
     *
     * @param businessObjectFormatEntity the business object format entity
     * @param attributes the attributes
     */
    private void updateBusinessObjectFormatAttributesHelper(BusinessObjectFormatEntity businessObjectFormatEntity, List<Attribute> attributes)
    {
        // Update the attributes.
        // Load all existing attribute entities in a map with a "lowercase" attribute name as the key for case insensitivity.
        Map<String, BusinessObjectFormatAttributeEntity> existingAttributeEntities = new HashMap<>();
        for (BusinessObjectFormatAttributeEntity attributeEntity : businessObjectFormatEntity.getAttributes())
        {
            String mapKey = attributeEntity.getName().toLowerCase();
            if (existingAttributeEntities.containsKey(mapKey))
            {
                throw new IllegalStateException(String.format("Found duplicate attribute with name \"%s\" for business object format {%s}.", mapKey,
                    businessObjectFormatHelper.businessObjectFormatEntityAltKeyToString(businessObjectFormatEntity)));
            }
            existingAttributeEntities.put(mapKey, attributeEntity);
        }
        // Process the list of attributes to determine that business object definition attribute entities should be created, updated, or deleted.
        List<BusinessObjectFormatAttributeEntity> createdAttributeEntities = new ArrayList<>();
        List<BusinessObjectFormatAttributeEntity> retainedAttributeEntities = new ArrayList<>();
        if (!CollectionUtils.isEmpty(attributes))
        {
            for (Attribute attribute : attributes)
            {
                // Use a "lowercase" attribute name for case insensitivity.
                String lowercaseAttributeName = attribute.getName().toLowerCase();
                if (existingAttributeEntities.containsKey(lowercaseAttributeName))
                {
                    // Check if the attribute value needs to be updated.
                    BusinessObjectFormatAttributeEntity attributeEntity = existingAttributeEntities.get(lowercaseAttributeName);
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
                    BusinessObjectFormatAttributeEntity attributeEntity = new BusinessObjectFormatAttributeEntity();
                    businessObjectFormatEntity.getAttributes().add(attributeEntity);
                    attributeEntity.setBusinessObjectFormat(businessObjectFormatEntity);
                    attributeEntity.setName(attribute.getName());
                    attributeEntity.setValue(attribute.getValue());

                    // Add this entity to the list of the newly created business object definition attribute entities.
                    createdAttributeEntities.add(attributeEntity);
                }
            }
        }

        // Remove any of the currently existing attribute entities that did not get onto the retained entities list.
        businessObjectFormatEntity.getAttributes().retainAll(retainedAttributeEntities);

        // Add all of the newly created business object definition attribute entities.
        businessObjectFormatEntity.getAttributes().addAll(createdAttributeEntities);
    }

    /**
     * Updates business object format attribute definitions
     *
     * @param businessObjectFormatEntity the business object format entity
     * @param attributeDefinitions the attributes
     */
    private void updateBusinessObjectFormatAttributeDefinitionsHelper(BusinessObjectFormatEntity businessObjectFormatEntity,
        List<AttributeDefinition> attributeDefinitions)
    {
        // Update the attribute definitions.
        // Load all existing attribute definition entities in a map with a "lowercase" attribute definition name as the key for case insensitivity.
        Map<String, BusinessObjectDataAttributeDefinitionEntity> existingAttributeDefinitionEntities =
            businessObjectFormatEntity.getAttributeDefinitions().stream()
                .collect(Collectors.toMap(attributeDefinition -> attributeDefinition.getName().toLowerCase(), attributeDefinition -> attributeDefinition));
        // Process the list of attribute definitions to determine that business object definition attribute entities should be created, updated, or deleted.
        List<BusinessObjectDataAttributeDefinitionEntity> createdAttributeDefinitionEntities = new ArrayList<>();
        List<BusinessObjectDataAttributeDefinitionEntity> retainedAttributeDefinitionEntities = new ArrayList<>();

        for (AttributeDefinition attributeDefinition : attributeDefinitions)
        {
            // Use a "lowercase" attribute name for case insensitivity.
            String lowercaseAttributeName = attributeDefinition.getName().toLowerCase();
            if (existingAttributeDefinitionEntities.containsKey(lowercaseAttributeName))
            {
                // Check if the attribute definition value needs to be updated.
                BusinessObjectDataAttributeDefinitionEntity businessObjectDataAttributeDefinitionEntity =
                    existingAttributeDefinitionEntities.get(lowercaseAttributeName);
                if (!attributeDefinition.isPublish().equals(businessObjectDataAttributeDefinitionEntity.getPublish()))
                {
                    // Update the business object attribute entity.
                    businessObjectDataAttributeDefinitionEntity.setPublish(attributeDefinition.isPublish());
                }

                // Add this entity to the list of business object definition attribute entities to be retained.
                retainedAttributeDefinitionEntities.add(businessObjectDataAttributeDefinitionEntity);
            }
            else
            {
                // Create a new business object attribute entity.
                BusinessObjectDataAttributeDefinitionEntity businessObjectDataAttributeDefinitionEntity = new BusinessObjectDataAttributeDefinitionEntity();
                businessObjectFormatEntity.getAttributeDefinitions().add(businessObjectDataAttributeDefinitionEntity);
                businessObjectDataAttributeDefinitionEntity.setBusinessObjectFormat(businessObjectFormatEntity);
                businessObjectDataAttributeDefinitionEntity.setName(attributeDefinition.getName());
                businessObjectDataAttributeDefinitionEntity.setPublish(BooleanUtils.isTrue(attributeDefinition.isPublish()));

                // Add this entity to the list of the newly created business object definition attribute entities.
                createdAttributeDefinitionEntities.add(businessObjectDataAttributeDefinitionEntity);
            }
        }

        // Remove any of the currently existing attribute entities that did not get onto the retained entities list.
        businessObjectFormatEntity.getAttributeDefinitions().retainAll(retainedAttributeDefinitionEntities);

        // Add all of the newly created business object definition attribute entities.
        businessObjectFormatEntity.getAttributeDefinitions().addAll(createdAttributeDefinitionEntities);
    }

    /**
     * Validates business object format attribute definitions
     *
     * @param attributeDefinitions the attribute definitions
     */
    private void validateAndTrimBusinessObjectFormatAttributeDefinitionsHelper(List<AttributeDefinition> attributeDefinitions)
    {
        Assert.notNull(attributeDefinitions, "A business object format attribute definitions list is required.");

        Map<String, AttributeDefinition> attributeDefinitionNameValidationMap = new HashMap<>();
        for (AttributeDefinition attributeDefinition : attributeDefinitions)
        {
            Assert.hasText(attributeDefinition.getName(), "An attribute definition name must be specified.");
            attributeDefinition.setName(attributeDefinition.getName().trim());

            // Ensure the attribute defination key isn't a duplicate by using a map with a "lowercase" name as the key for case insensitivity.
            String lowercaseAttributeDefinitionName = attributeDefinition.getName().toLowerCase();
            if (attributeDefinitionNameValidationMap.containsKey(lowercaseAttributeDefinitionName))
            {
                throw new IllegalArgumentException(String.format("Duplicate attribute definition name \"%s\" found.", attributeDefinition.getName()));
            }
            attributeDefinitionNameValidationMap.put(lowercaseAttributeDefinitionName, attributeDefinition);
        }
    }

    /**
     * Removes the leading and trailing white spaces in the document schema
     *
     * @param documentSchema - document schema
     *
     * @return document schema with leading and trailing white spaces removed.
     */
    private String getTrimmedDocumentSchema(String documentSchema)
    {
        return documentSchema != null ? documentSchema.trim() : documentSchema;
    }
}
