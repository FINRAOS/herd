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
package org.finra.herd.service.helper;

import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_UPDATE;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.BusinessObjectDataDao;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectFormatDao;
import org.finra.herd.dao.RetentionTypeDao;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectFormat;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatExternalInterfaceEntity;
import org.finra.herd.model.jpa.ExternalInterfaceEntity;
import org.finra.herd.model.jpa.RetentionTypeEntity;

/**
 * Helper for business object format related operations which require DAO.
 */
@Component
public class BusinessObjectFormatDaoHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessObjectFormatDaoHelper.class);

    @Autowired
    private BusinessObjectDataDao businessObjectDataDao;

    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private BusinessObjectFormatHelper businessObjectFormatHelper;

    @Autowired
    private RetentionTypeDao retentionTypeDao;

    @Autowired
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

    /**
     * Deletes a business object format.
     *
     * @param businessObjectFormatKey the business object format alternate key
     *
     * @return the business object format that was deleted
     */
    public BusinessObjectFormat deleteBusinessObjectFormat(BusinessObjectFormatKey businessObjectFormatKey)
    {
        // Perform validation and trim the alternate key parameters.
        businessObjectFormatHelper.validateBusinessObjectFormatKey(businessObjectFormatKey);

        // Retrieve and ensure that a business object format exists.
        BusinessObjectFormatEntity businessObjectFormatEntity = getBusinessObjectFormatEntity(businessObjectFormatKey);

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

        // Get the external interface references for this business object format.
        List<ExternalInterfaceEntity> externalInterfaceEntities = new ArrayList<>();
        for (BusinessObjectFormatExternalInterfaceEntity businessObjectFormatExternalInterfaceEntity : businessObjectFormatEntity
            .getBusinessObjectFormatExternalInterfaces())
        {
            externalInterfaceEntities.add(businessObjectFormatExternalInterfaceEntity.getExternalInterface());
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

                // Create external interface references for the previous version of the business object format.
                for (ExternalInterfaceEntity externalInterfaceEntity : externalInterfaceEntities)
                {
                    // Creates a business object format to external interface mapping entity.
                    BusinessObjectFormatExternalInterfaceEntity businessObjectFormatExternalInterfaceEntity = new BusinessObjectFormatExternalInterfaceEntity();
                    previousVersionBusinessObjectFormatEntity.getBusinessObjectFormatExternalInterfaces().add(businessObjectFormatExternalInterfaceEntity);
                    businessObjectFormatExternalInterfaceEntity.setBusinessObjectFormat(previousVersionBusinessObjectFormatEntity);
                    businessObjectFormatExternalInterfaceEntity.setExternalInterface(externalInterfaceEntity);
                }

                // Save the updated entity.
                businessObjectFormatDao.saveAndRefresh(previousVersionBusinessObjectFormatEntity);
            }
        }

        // Notify the search index that a business object definition must be updated.
        LOGGER.info("Modify the business object definition in the search index associated with the business object definition format being deleted." +
            " businessObjectDefinitionId=\"{}\", searchIndexUpdateType=\"{}\"", businessObjectDefinitionEntity.getId(), SEARCH_INDEX_UPDATE_TYPE_UPDATE);
        searchIndexUpdateHelper.modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);

        return deletedBusinessObjectFormat;
    }

    /**
     * Gets a business object format entity based on the alternate key and makes sure that it exists. If a format version isn't specified in the business object
     * format alternate key, the latest available format version will be used.
     *
     * @param businessObjectFormatKey the business object format key
     *
     * @return the business object format entity
     * @throws ObjectNotFoundException if the business object format entity doesn't exist
     */
    public BusinessObjectFormatEntity getBusinessObjectFormatEntity(BusinessObjectFormatKey businessObjectFormatKey) throws ObjectNotFoundException
    {
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey);

        if (businessObjectFormatEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Business object format with namespace \"%s\", business object definition name \"%s\", " +
                    "format usage \"%s\", format file type \"%s\", and format version \"%d\" doesn't exist.", businessObjectFormatKey.getNamespace(),
                businessObjectFormatKey.getBusinessObjectDefinitionName(), businessObjectFormatKey.getBusinessObjectFormatUsage(),
                businessObjectFormatKey.getBusinessObjectFormatFileType(), businessObjectFormatKey.getBusinessObjectFormatVersion()));
        }

        return businessObjectFormatEntity;
    }

    /**
     * Gets record retention type entity form retention type code
     *
     * @param retentionTypeCode retention type code
     *
     * @return the retention type entity
     */
    public RetentionTypeEntity getRecordRetentionTypeEntity(String retentionTypeCode)
    {
        RetentionTypeEntity recordRetentionTypeEntity = retentionTypeDao.getRetentionTypeByCode(retentionTypeCode);

        if (recordRetentionTypeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Record retention type with code \"%s\" doesn't exist.", retentionTypeCode));
        }

        return recordRetentionTypeEntity;
    }

    /**
     * Checks if business object data published attributes change event notification feature is enabled for this business object format and it contains at least
     * one publishable attribute definition.
     *
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return true if business object data published attributes change event notification feature is enabled for this business object format and it contains at
     * least one publishable attribute definition, otherwise false
     */
    public boolean isBusinessObjectDataPublishedAttributesChangeEventNotificationRequired(BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        return BooleanUtils.isTrue(businessObjectFormatEntity.isEnableBusinessObjectDataPublishedAttributesChangeEventNotification()) &&
            checkBusinessObjectFormatContainsPublishableAttributeDefinitions(businessObjectFormatEntity);
    }

    /**
     * Checks if business object format contains at least one publishable attribute definition.
     *
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return true when business object format has at least one attribute definition with publish flag set to true, otherwise false
     */
    public boolean checkBusinessObjectFormatContainsPublishableAttributeDefinitions(BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        if (!CollectionUtils.isEmpty(businessObjectFormatEntity.getAttributeDefinitions()))
        {
            for (BusinessObjectDataAttributeDefinitionEntity businessObjectDataAttributeDefinitionEntity : businessObjectFormatEntity.getAttributeDefinitions())
            {
                if (BooleanUtils.isTrue(businessObjectDataAttributeDefinitionEntity.getPublish()))
                {
                    return true;
                }
            }
        }

        return false;
    }
}
