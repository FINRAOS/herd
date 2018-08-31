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
package org.finra.herd.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.model.api.xml.Attribute;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.SchemaColumn;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;
import org.finra.herd.model.jpa.SchemaColumnEntity;

@Component
public class BusinessObjectFormatDaoTestHelper
{
    @Autowired
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Autowired
    private BusinessObjectDefinitionDaoTestHelper businessObjectDefinitionDaoTestHelper;

    @Autowired
    private BusinessObjectFormatDao businessObjectFormatDao;

    @Autowired
    private FileTypeDao fileTypeDao;

    @Autowired
    private FileTypeDaoTestHelper fileTypeDaoTestHelper;

    @Autowired
    private PartitionKeyGroupDao partitionKeyGroupDao;

    @Autowired
    private PartitionKeyGroupDaoTestHelper partitionKeyGroupDaoTestHelper;

    /**
     * Creates and persists a new business object data attribute definition entity.
     *
     * @param namespaceCode the namespace code
     * @param businessObjectDefinitionName the business object definition name
     * @param businessObjectFormatUsage the business object format usage
     * @param businessObjectFormatFileType the business object format file type
     * @param businessObjectFormatVersion the business object format version
     * @param businessObjectDataAttributeName the business object data attribute name
     *
     * @return the newly created business object data attribute definition entity.
     */
    public BusinessObjectDataAttributeDefinitionEntity createBusinessObjectDataAttributeDefinitionEntity(String namespaceCode,
        String businessObjectDefinitionName, String businessObjectFormatUsage, String businessObjectFormatFileType, Integer businessObjectFormatVersion,
        String businessObjectDataAttributeName)
    {
        // Create a business object format entity if it does not exist.
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectFormatDao.getBusinessObjectFormatByAltKey(
            new BusinessObjectFormatKey(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                businessObjectFormatVersion));
        if (businessObjectFormatEntity == null)
        {
            businessObjectFormatEntity =
                createBusinessObjectFormatEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, businessObjectFormatFileType,
                    businessObjectFormatVersion, AbstractDaoTest.FORMAT_DESCRIPTION, AbstractDaoTest.FORMAT_DOCUMENT_SCHEMA, true,
                    AbstractDaoTest.PARTITION_KEY);
        }

        return createBusinessObjectDataAttributeDefinitionEntity(businessObjectFormatEntity, businessObjectDataAttributeName,
            AbstractDaoTest.NO_PUBLISH_ATTRIBUTE);
    }

    /**
     * Creates and persists a new business object data attribute definition entity.
     *
     * @param businessObjectFormatEntity the business object format entity
     * @param businessObjectDataAttributeName the business object data attribute name
     * @param publishBusinessObjectDataAttribute specifies if this business object data attribute should be published in notification messages
     *
     * @return the newly created business object data attribute definition entity.
     */
    public BusinessObjectDataAttributeDefinitionEntity createBusinessObjectDataAttributeDefinitionEntity(BusinessObjectFormatEntity businessObjectFormatEntity,
        String businessObjectDataAttributeName, boolean publishBusinessObjectDataAttribute)
    {
        // Create a new business object data attribute definition entity.
        BusinessObjectDataAttributeDefinitionEntity businessObjectDataAttributeDefinitionEntity = new BusinessObjectDataAttributeDefinitionEntity();
        businessObjectDataAttributeDefinitionEntity.setBusinessObjectFormat(businessObjectFormatEntity);
        businessObjectDataAttributeDefinitionEntity.setName(businessObjectDataAttributeName);
        businessObjectDataAttributeDefinitionEntity.setPublish(publishBusinessObjectDataAttribute);

        // Update the parent entity.
        businessObjectFormatEntity.getAttributeDefinitions().add(businessObjectDataAttributeDefinitionEntity);
        businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);

        return businessObjectDataAttributeDefinitionEntity;
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    public BusinessObjectFormatEntity createBusinessObjectFormatEntity(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String fileType, Integer businessObjectFormatVersion, String businessObjectFormatDescription,
        String businessObjectFormatDocumentSchema, Boolean businessObjectFormatLatestVersion, String businessObjectFormatPartitionKey,
        String partitionKeyGroupName)
    {
        return createBusinessObjectFormatEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion,
            businessObjectFormatDescription, businessObjectFormatDocumentSchema, businessObjectFormatLatestVersion, businessObjectFormatPartitionKey,
            partitionKeyGroupName, AbstractDaoTest.NO_ATTRIBUTES);
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    public BusinessObjectFormatEntity createBusinessObjectFormatEntity(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String fileType, Integer businessObjectFormatVersion, String businessObjectFormatDescription,
        String businessObjectFormatDocumentSchema, Boolean businessObjectFormatLatestVersion, String businessObjectFormatPartitionKey,
        String partitionKeyGroupName, List<Attribute> attributes)
    {
        return createBusinessObjectFormatEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion,
            businessObjectFormatDescription, businessObjectFormatDocumentSchema, businessObjectFormatLatestVersion, businessObjectFormatPartitionKey,
            partitionKeyGroupName, attributes, null, null, null, null, null);
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    public BusinessObjectFormatEntity createBusinessObjectFormatEntity(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String fileType, Integer businessObjectFormatVersion, String businessObjectFormatDescription,
        String businessObjectFormatDocumentSchema, Boolean businessObjectFormatLatestVersion, String businessObjectFormatPartitionKey,
        String partitionKeyGroupName, List<Attribute> attributes, String schemaDelimiterCharacter, String schemaEscapeCharacter, String schemaNullValue,
        List<SchemaColumn> schemaColumns, List<SchemaColumn> partitionColumns)
    {
        // Create a business object definition entity if it does not exist.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
            businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(namespaceCode, businessObjectDefinitionName));
        if (businessObjectDefinitionEntity == null)
        {
            businessObjectDefinitionEntity = businessObjectDefinitionDaoTestHelper
                .createBusinessObjectDefinitionEntity(namespaceCode, businessObjectDefinitionName, AbstractDaoTest.DATA_PROVIDER_NAME, null);
        }

        // Create a business object format file type entity if it does not exist.
        FileTypeEntity fileTypeEntity = fileTypeDao.getFileTypeByCode(fileType);
        if (fileTypeEntity == null)
        {
            fileTypeEntity = fileTypeDaoTestHelper.createFileTypeEntity(fileType, null);
        }

        // If partition key group was specified, check if we need to create an entity for it first.
        PartitionKeyGroupEntity partitionKeyGroupEntity = null;
        if (StringUtils.isNotBlank(partitionKeyGroupName))
        {
            partitionKeyGroupEntity = partitionKeyGroupDao.getPartitionKeyGroupByName(partitionKeyGroupName);
            if (partitionKeyGroupEntity == null)
            {
                partitionKeyGroupEntity = partitionKeyGroupDaoTestHelper.createPartitionKeyGroupEntity(partitionKeyGroupName);
            }
        }

        return createBusinessObjectFormatEntity(businessObjectDefinitionEntity, businessObjectFormatUsage, fileTypeEntity, businessObjectFormatVersion,
            businessObjectFormatDescription, businessObjectFormatDocumentSchema, businessObjectFormatLatestVersion, businessObjectFormatPartitionKey,
            partitionKeyGroupEntity, attributes, schemaDelimiterCharacter, schemaEscapeCharacter, schemaNullValue, schemaColumns, partitionColumns);
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    public BusinessObjectFormatEntity createBusinessObjectFormatEntity(BusinessObjectDefinitionEntity businessObjectDefinitionEntity,
        String businessObjectFormatUsage, FileTypeEntity fileTypeEntity, Integer businessObjectFormatVersion, String businessObjectFormatDescription,
        String businessObjectFormatDocumentSchema, Boolean businessObjectFormatLatestVersion, String businessObjectFormatPartitionKey,
        PartitionKeyGroupEntity partitionKeyGroupEntity, List<Attribute> attributes, String schemaDelimiterCharacter, String schemaEscapeCharacter,
        String schemaNullValue, List<SchemaColumn> schemaColumns, List<SchemaColumn> partitionColumns)
    {
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();

        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectFormatEntity.setDescription(businessObjectFormatDescription);
        businessObjectFormatEntity.setDocumentSchema(businessObjectFormatDocumentSchema);
        businessObjectFormatEntity.setFileType(fileTypeEntity);
        businessObjectFormatEntity.setBusinessObjectFormatVersion(businessObjectFormatVersion);
        businessObjectFormatEntity.setLatestVersion(businessObjectFormatLatestVersion);
        businessObjectFormatEntity.setUsage(businessObjectFormatUsage);
        businessObjectFormatEntity.setPartitionKey(businessObjectFormatPartitionKey);
        businessObjectFormatEntity.setPartitionKeyGroup(partitionKeyGroupEntity);

        // Create the attributes if they are specified.
        if (!CollectionUtils.isEmpty(attributes))
        {
            List<BusinessObjectFormatAttributeEntity> attributeEntities = new ArrayList<>();
            businessObjectFormatEntity.setAttributes(attributeEntities);
            for (Attribute attribute : attributes)
            {
                BusinessObjectFormatAttributeEntity attributeEntity = new BusinessObjectFormatAttributeEntity();
                attributeEntities.add(attributeEntity);
                attributeEntity.setBusinessObjectFormat(businessObjectFormatEntity);
                attributeEntity.setName(attribute.getName());
                attributeEntity.setValue(attribute.getValue());
            }
        }

        if (schemaColumns != null && !schemaColumns.isEmpty())
        {
            businessObjectFormatEntity.setDelimiter(schemaDelimiterCharacter);
            businessObjectFormatEntity.setEscapeCharacter(schemaEscapeCharacter);
            businessObjectFormatEntity.setNullValue(schemaNullValue);

            List<SchemaColumnEntity> schemaColumnEntities = new ArrayList<>();
            businessObjectFormatEntity.setSchemaColumns(schemaColumnEntities);

            int columnPosition = 1;
            for (SchemaColumn schemaColumn : schemaColumns)
            {
                SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
                schemaColumnEntities.add(schemaColumnEntity);
                schemaColumnEntity.setBusinessObjectFormat(businessObjectFormatEntity);
                schemaColumnEntity.setPosition(columnPosition);
                schemaColumnEntity.setPartitionLevel(null);
                schemaColumnEntity.setName(schemaColumn.getName());
                schemaColumnEntity.setType(schemaColumn.getType());
                schemaColumnEntity.setSize(schemaColumn.getSize());
                schemaColumnEntity.setDescription(schemaColumn.getDescription());
                schemaColumnEntity.setRequired(schemaColumn.isRequired());
                schemaColumnEntity.setDefaultValue(schemaColumn.getDefaultValue());
                columnPosition++;
            }

            if (partitionColumns != null && !partitionColumns.isEmpty())
            {
                int partitionLevel = 1;
                for (SchemaColumn schemaColumn : partitionColumns)
                {
                    // Check if this partition column belongs to the list of regular schema columns.
                    int schemaColumnIndex = schemaColumns.indexOf(schemaColumn);
                    if (schemaColumnIndex >= 0)
                    {
                        // Retrieve the relative column entity and set its partition level.
                        schemaColumnEntities.get(schemaColumnIndex).setPartitionLevel(partitionLevel);
                    }
                    else
                    {
                        // Add this partition column as a new schema column entity.
                        SchemaColumnEntity schemaColumnEntity = new SchemaColumnEntity();
                        schemaColumnEntities.add(schemaColumnEntity);
                        schemaColumnEntity.setBusinessObjectFormat(businessObjectFormatEntity);
                        schemaColumnEntity.setPosition(null);
                        schemaColumnEntity.setPartitionLevel(partitionLevel);
                        schemaColumnEntity.setName(schemaColumn.getName());
                        schemaColumnEntity.setType(schemaColumn.getType());
                        schemaColumnEntity.setSize(schemaColumn.getSize());
                        schemaColumnEntity.setDescription(schemaColumn.getDescription());
                        schemaColumnEntity.setRequired(schemaColumn.isRequired());
                        schemaColumnEntity.setDefaultValue(schemaColumn.getDefaultValue());
                    }
                    partitionLevel++;
                }
            }
        }

        return businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    public BusinessObjectFormatEntity createBusinessObjectFormatEntity(boolean includeAttributeDefinition)
    {
        BusinessObjectFormatEntity businessObjectFormatEntity = new BusinessObjectFormatEntity();
        businessObjectFormatEntity.setBusinessObjectDefinition(businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinition());
        businessObjectFormatEntity.setDescription("test");
        businessObjectFormatEntity.setFileType(fileTypeDaoTestHelper.createFileTypeEntity());
        businessObjectFormatEntity.setBusinessObjectFormatVersion(0);
        businessObjectFormatEntity.setLatestVersion(true);
        businessObjectFormatEntity.setUsage("PRC");
        businessObjectFormatEntity.setPartitionKey("testPartitionKey");

        if (includeAttributeDefinition)
        {
            List<BusinessObjectDataAttributeDefinitionEntity> attributeDefinitionEntities = new ArrayList<>();
            businessObjectFormatEntity.setAttributeDefinitions(attributeDefinitionEntities);
            BusinessObjectDataAttributeDefinitionEntity attributeDefinitionEntity = new BusinessObjectDataAttributeDefinitionEntity();
            attributeDefinitionEntities.add(attributeDefinitionEntity);
            attributeDefinitionEntity.setBusinessObjectFormat(businessObjectFormatEntity);
            attributeDefinitionEntity.setName(AbstractDaoTest.ATTRIBUTE_NAME_1_MIXED_CASE);
        }

        return businessObjectFormatDao.saveAndRefresh(businessObjectFormatEntity);
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    public BusinessObjectFormatEntity createBusinessObjectFormatEntity(BusinessObjectFormatKey businessObjectFormatKey, String businessObjectFormatDescription,
        String businessObjectFormatDocumentSchema, Boolean businessObjectFormatLatestVersion, String businessObjectFormatPartitionKey)
    {
        return createBusinessObjectFormatEntity(businessObjectFormatKey.getNamespace(), businessObjectFormatKey.getBusinessObjectDefinitionName(),
            businessObjectFormatKey.getBusinessObjectFormatUsage(), businessObjectFormatKey.getBusinessObjectFormatFileType(),
            businessObjectFormatKey.getBusinessObjectFormatVersion(), businessObjectFormatDescription, businessObjectFormatDocumentSchema,
            businessObjectFormatLatestVersion, businessObjectFormatPartitionKey);
    }

    /**
     * Creates and persists a new business object format entity.
     *
     * @return the newly created business object format entity.
     */
    public BusinessObjectFormatEntity createBusinessObjectFormatEntity(String namespaceCode, String businessObjectDefinitionName,
        String businessObjectFormatUsage, String fileType, Integer businessObjectFormatVersion, String businessObjectFormatDescription,
        String businessObjectFormatDocumentSchema, Boolean businessObjectFormatLatestVersion, String businessObjectFormatPartitionKey)
    {
        return createBusinessObjectFormatEntity(namespaceCode, businessObjectDefinitionName, businessObjectFormatUsage, fileType, businessObjectFormatVersion,
            businessObjectFormatDescription, businessObjectFormatDocumentSchema, businessObjectFormatLatestVersion, businessObjectFormatPartitionKey, null);
    }

    /**
     * Returns a list of test business object format keys expected to be returned by getBusinessObjectFormatKeys() method.
     *
     * @return the list of expected business object format keys
     */
    public List<BusinessObjectFormatKey> getExpectedBusinessObjectFormatKeys()
    {
        List<BusinessObjectFormatKey> keys = new ArrayList<>();

        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.SECOND_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE_2, AbstractDaoTest.INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE_2,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE_2,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.SECOND_FORMAT_VERSION));

        return keys;
    }

    /**
     * Returns a list of test business object format keys expected to be returned by getBusinessObjectFormatKeys() method with the
     * latestBusinessObjectFormatVersion flag set to "true".
     *
     * @return the list of expected business object format keys
     */
    public List<BusinessObjectFormatKey> getExpectedBusinessObjectFormatLatestVersionKeys()
    {
        List<BusinessObjectFormatKey> keys = new ArrayList<>();

        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.SECOND_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE_2, AbstractDaoTest.INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE_2,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.SECOND_FORMAT_VERSION));

        return keys;
    }

    /**
     * Returns a list of test business object format keys.
     *
     * @return the list of test business object format keys
     */
    public List<BusinessObjectFormatKey> getTestBusinessObjectFormatKeys()
    {
        List<BusinessObjectFormatKey> keys = new ArrayList<>();

        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE_2,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE_2,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.SECOND_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE_2, AbstractDaoTest.INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.SECOND_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE, AbstractDaoTest.BDEF_NAME_2, AbstractDaoTest.FORMAT_USAGE_CODE,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE_2, AbstractDaoTest.BDEF_NAME, AbstractDaoTest.FORMAT_USAGE_CODE,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.INITIAL_FORMAT_VERSION));
        keys.add(new BusinessObjectFormatKey(AbstractDaoTest.NAMESPACE_2, AbstractDaoTest.BDEF_NAME_2, AbstractDaoTest.FORMAT_USAGE_CODE,
            AbstractDaoTest.FORMAT_FILE_TYPE_CODE, AbstractDaoTest.INITIAL_FORMAT_VERSION));

        return keys;
    }
}
