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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import org.finra.herd.dao.HerdDao;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.MethodNotAllowedException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDataAttributeKey;
import org.finra.herd.model.api.xml.BusinessObjectDataKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectFormatKey;
import org.finra.herd.model.api.xml.CustomDdlKey;
import org.finra.herd.model.api.xml.EmrClusterDefinitionKey;
import org.finra.herd.model.api.xml.PartitionKeyGroupKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectDataAttributeEntity;
import org.finra.herd.model.jpa.BusinessObjectDataEntity;
import org.finra.herd.model.jpa.BusinessObjectDataStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.BusinessObjectFormatEntity;
import org.finra.herd.model.jpa.ConfigurationEntity;
import org.finra.herd.model.jpa.CustomDdlEntity;
import org.finra.herd.model.jpa.DataProviderEntity;
import org.finra.herd.model.jpa.EmrClusterDefinitionEntity;
import org.finra.herd.model.jpa.FileTypeEntity;
import org.finra.herd.model.jpa.JmsMessageEntity;
import org.finra.herd.model.jpa.JobDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.model.jpa.NotificationEventTypeEntity;
import org.finra.herd.model.jpa.PartitionKeyGroupEntity;

/**
 * A helper class for HerdDao related code.
 */
@Component
public class HerdDaoHelper
{
    @Autowired
    private HerdDao herdDao;

    @Autowired
    private HerdHelper herdHelper;

    @Autowired
    private HerdStringHelper herdStringHelper;

    /**
     * Gets a namespace entity and ensure it exists.
     *
     * @param namespace the namespace (case insensitive)
     *
     * @return the namespace entity
     * @throws ObjectNotFoundException if the namespace entity doesn't exist
     */
    public NamespaceEntity getNamespaceEntity(String namespace) throws ObjectNotFoundException
    {
        NamespaceEntity namespaceEntity = herdDao.getNamespaceByCd(namespace);

        if (namespaceEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Namespace \"%s\" doesn't exist.", namespace));
        }

        return namespaceEntity;
    }

    /**
     * Gets a data provider entity and ensure it exists.
     *
     * @param dataProviderName the data provider name (case insensitive)
     *
     * @return the data provider entity
     * @throws ObjectNotFoundException if the data provider entity doesn't exist
     */
    public DataProviderEntity getDataProviderEntity(String dataProviderName) throws ObjectNotFoundException
    {
        DataProviderEntity dataProviderEntity = herdDao.getDataProviderByName(dataProviderName);

        if (dataProviderEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Data provider with name \"%s\" doesn't exist.", dataProviderName));
        }

        return dataProviderEntity;
    }

    /**
     * Retrieves a business object definition entity by it's key and ensure it exists.
     *
     * @param businessObjectDefinitionKey the business object definition name (case-insensitive)
     *
     * @return the business object definition entity
     * @throws ObjectNotFoundException if the business object definition entity doesn't exist
     */
    public BusinessObjectDefinitionEntity getBusinessObjectDefinitionEntity(BusinessObjectDefinitionKey businessObjectDefinitionKey)
        throws ObjectNotFoundException
    {
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = herdDao.getBusinessObjectDefinitionByKey(businessObjectDefinitionKey);

        if (businessObjectDefinitionEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Business object definition with name \"%s\" doesn't exist for namespace \"%s\".",
                businessObjectDefinitionKey.getBusinessObjectDefinitionName(), businessObjectDefinitionKey.getNamespace()));
        }

        return businessObjectDefinitionEntity;
    }

    /**
     * Gets the file type entity and ensure it exists.
     *
     * @param fileType the file type (case insensitive)
     *
     * @return the file type entity
     * @throws ObjectNotFoundException if the file type entity doesn't exist
     */
    public FileTypeEntity getFileTypeEntity(String fileType) throws ObjectNotFoundException
    {
        FileTypeEntity fileTypeEntity = herdDao.getFileTypeByCode(fileType);

        if (fileTypeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("File type with code \"%s\" doesn't exist.", fileType));
        }

        return fileTypeEntity;
    }

    /**
     * Gets the partition key group entity and ensure it exists.
     *
     * @param partitionKeyGroupKey the partition key group key
     *
     * @return the partition key group entity
     * @throws ObjectNotFoundException if the partition key group entity doesn't exist
     */
    public PartitionKeyGroupEntity getPartitionKeyGroupEntity(PartitionKeyGroupKey partitionKeyGroupKey) throws ObjectNotFoundException
    {
        return getPartitionKeyGroupEntity(partitionKeyGroupKey.getPartitionKeyGroupName());
    }

    /**
     * Gets the partition key group entity and ensure it exists.
     *
     * @param partitionKeyGroupName the partition key group name (case insensitive)
     *
     * @return the partition key group entity
     * @throws ObjectNotFoundException if the partition key group entity doesn't exist
     */
    public PartitionKeyGroupEntity getPartitionKeyGroupEntity(String partitionKeyGroupName) throws ObjectNotFoundException
    {
        PartitionKeyGroupEntity partitionKeyGroupEntity = herdDao.getPartitionKeyGroupByName(partitionKeyGroupName);

        if (partitionKeyGroupEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Partition key group \"%s\" doesn't exist.", partitionKeyGroupName));
        }

        return partitionKeyGroupEntity;
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
        BusinessObjectFormatEntity businessObjectFormatEntity = herdDao.getBusinessObjectFormatByAltKey(businessObjectFormatKey);

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
     * Creates a business object format key from specified business object format entity.
     *
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return the business object format key
     */
    public BusinessObjectFormatKey getBusinessObjectFormatKey(BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        BusinessObjectFormatKey businessObjectFormatKey = new BusinessObjectFormatKey();

        businessObjectFormatKey.setNamespace(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode());
        businessObjectFormatKey.setBusinessObjectDefinitionName(businessObjectFormatEntity.getBusinessObjectDefinition().getName());
        businessObjectFormatKey.setBusinessObjectFormatUsage(businessObjectFormatEntity.getUsage());
        businessObjectFormatKey.setBusinessObjectFormatFileType(businessObjectFormatEntity.getFileType().getCode());
        businessObjectFormatKey.setBusinessObjectFormatVersion(businessObjectFormatEntity.getBusinessObjectFormatVersion());

        return businessObjectFormatKey;
    }

    /**
     * Returns a string representation of the alternate key values for the business object format.
     *
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return the string representation of the alternate key values for the business object format entity
     */
    public String businessObjectFormatEntityAltKeyToString(BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        return String.format("namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
            "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d",
            businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
            businessObjectFormatEntity.getFileType().getCode(), businessObjectFormatEntity.getBusinessObjectFormatVersion());
    }

    /**
     * Determines if the specified business object data attribute is a required attribute or not.
     *
     * @param businessObjectAttributeName the name of the business object attribute
     * @param businessObjectFormatEntity the business object format entity
     *
     * @return true if this business object data attribute is a required attribute, otherwise false
     */
    public boolean isBusinessObjectDataAttributeRequired(String businessObjectAttributeName, BusinessObjectFormatEntity businessObjectFormatEntity)
    {
        boolean required = false;

        for (BusinessObjectDataAttributeDefinitionEntity attributeDefinitionEntity : businessObjectFormatEntity.getAttributeDefinitions())
        {
            if (businessObjectAttributeName.equalsIgnoreCase(attributeDefinitionEntity.getName()))
            {
                required = true;
                break;
            }
        }

        return required;
    }

    /**
     * Gets a custom DDL entity based on the key and makes sure that it exists.
     *
     * @param customDdlKey the custom DDL key
     *
     * @return the custom DDL entity
     * @throws ObjectNotFoundException if the business object format entity doesn't exist
     */
    public CustomDdlEntity getCustomDdlEntity(CustomDdlKey customDdlKey) throws ObjectNotFoundException
    {
        CustomDdlEntity customDdlEntity = herdDao.getCustomDdlByKey(customDdlKey);

        if (customDdlEntity == null)
        {
            throw new ObjectNotFoundException(String.format(
                "Custom DDL with name \"%s\" does not exist for business object format with namespace \"%s\", business object definition name \"%s\", " +
                    "format usage \"%s\", format file type \"%s\", and format version \"%d\".", customDdlKey.getCustomDdlName(), customDdlKey.getNamespace(),
                customDdlKey.getBusinessObjectDefinitionName(), customDdlKey.getBusinessObjectFormatUsage(), customDdlKey.getBusinessObjectFormatFileType(),
                customDdlKey.getBusinessObjectFormatVersion()));
        }

        return customDdlEntity;
    }

    /**
     * Gets a business object status entity and ensure it exists.
     *
     * @param code the code (case insensitive)
     *
     * @return the Business Object Data Status Entity
     * @throws ObjectNotFoundException if the status entity doesn't exist
     */
    public BusinessObjectDataStatusEntity getBusinessObjectDataStatusEntity(String code) throws ObjectNotFoundException
    {
        BusinessObjectDataStatusEntity businessObjectDataStatusEntity = herdDao.getBusinessObjectDataStatusByCode(code);

        if (businessObjectDataStatusEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Business object data status \"%s\" doesn't exist.", code));
        }

        return businessObjectDataStatusEntity;
    }

    /**
     * Gets business object data based on the key information.
     *
     * @param businessObjectDataKey the business object data key.
     *
     * @return the business object data.
     */
    public BusinessObjectDataEntity getBusinessObjectDataEntity(BusinessObjectDataKey businessObjectDataKey)
    {
        // Retrieve the business object data entity regardless of its status.
        return getBusinessObjectDataEntityByKeyAndStatus(businessObjectDataKey, null);
    }

    /**
     * Retrieves business object data by it's key. If a format version isn't specified, the latest available format version (for this partition value) will be
     * used. If a business object data version isn't specified, the latest data version based on the specified business object data status is returned. When
     * both business object data version and business object data status are not specified, the latest data version for each set of partition values will be
     * used regardless of the status.
     *
     * @param businessObjectDataKey the business object data key
     * @param businessObjectDataStatus the business object data status. This parameter is ignored when the business object data version is specified.
     *
     * @return the business object data
     */
    public BusinessObjectDataEntity getBusinessObjectDataEntityByKeyAndStatus(BusinessObjectDataKey businessObjectDataKey, String businessObjectDataStatus)
    {
        // Get the business object data based on the specified parameters.
        BusinessObjectDataEntity businessObjectDataEntity = herdDao.getBusinessObjectDataByAltKeyAndStatus(businessObjectDataKey, businessObjectDataStatus);

        // Make sure that business object data exists.
        if (businessObjectDataEntity == null)
        {
            throw new ObjectNotFoundException(
                String.format("Business object data {namespace: \"%s\", businessObjectDefinitionName: \"%s\", businessObjectFormatUsage: \"%s\", " +
                    "businessObjectFormatFileType: \"%s\", businessObjectFormatVersion: %d, businessObjectDataPartitionValue: \"%s\", " +
                    "businessObjectDataSubPartitionValues: \"%s\", businessObjectDataVersion: %d, businessObjectDataStatus: \"%s\"} doesn't exist.",
                    businessObjectDataKey.getNamespace(), businessObjectDataKey.getBusinessObjectDefinitionName(),
                    businessObjectDataKey.getBusinessObjectFormatUsage(), businessObjectDataKey.getBusinessObjectFormatFileType(),
                    businessObjectDataKey.getBusinessObjectFormatVersion(), businessObjectDataKey.getPartitionValue(),
                    CollectionUtils.isEmpty(businessObjectDataKey.getSubPartitionValues()) ? ""
                        : StringUtils.join(businessObjectDataKey.getSubPartitionValues(), ","), businessObjectDataKey.getBusinessObjectDataVersion(),
                    businessObjectDataStatus));
        }

        // Return the retrieved business object data entity.
        return businessObjectDataEntity;
    }

    /**
     * Returns a string representation of the alternate key values for the business object data entity.
     *
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the string representation of the alternate key values for the business object data entity
     */
    public String businessObjectDataEntityAltKeyToString(BusinessObjectDataEntity businessObjectDataEntity)
    {
        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectDataEntity.getBusinessObjectFormat();

        return herdHelper.businessObjectDataKeyToString(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode(),
            businessObjectFormatEntity.getBusinessObjectDefinition().getName(), businessObjectFormatEntity.getUsage(),
            businessObjectFormatEntity.getFileType().getCode(), businessObjectFormatEntity.getBusinessObjectFormatVersion(),
            businessObjectDataEntity.getPartitionValue(), herdHelper.getSubPartitionValues(businessObjectDataEntity), businessObjectDataEntity.getVersion());
    }

    /**
     * Returns a business object data key for the business object data entity.
     *
     * @param businessObjectDataEntity the business object data entity
     *
     * @return the business object data key for the business object data entity
     */
    public BusinessObjectDataKey getBusinessObjectDataKey(BusinessObjectDataEntity businessObjectDataEntity)
    {
        BusinessObjectDataKey businessObjectDataKey = new BusinessObjectDataKey();

        BusinessObjectFormatEntity businessObjectFormatEntity = businessObjectDataEntity.getBusinessObjectFormat();
        businessObjectDataKey.setNamespace(businessObjectFormatEntity.getBusinessObjectDefinition().getNamespace().getCode());
        businessObjectDataKey.setBusinessObjectDefinitionName(businessObjectFormatEntity.getBusinessObjectDefinition().getName());
        businessObjectDataKey.setBusinessObjectFormatUsage(businessObjectFormatEntity.getUsage());
        businessObjectDataKey.setBusinessObjectFormatFileType(businessObjectFormatEntity.getFileType().getCode());
        businessObjectDataKey.setBusinessObjectFormatVersion(businessObjectFormatEntity.getBusinessObjectFormatVersion());
        businessObjectDataKey.setPartitionValue(businessObjectDataEntity.getPartitionValue());
        businessObjectDataKey.setSubPartitionValues(herdHelper.getSubPartitionValues(businessObjectDataEntity));
        businessObjectDataKey.setBusinessObjectDataVersion(businessObjectDataEntity.getVersion());

        return businessObjectDataKey;
    }

    /**
     * Returns primary partition or subpartition value specified by the partition column position.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param partitionColumnPosition the position of the partition column (one-based numbering)
     *
     * @return the value of the partition identified by the partition column position
     */
    public String getPartitionValue(BusinessObjectDataEntity businessObjectDataEntity, int partitionColumnPosition)
    {
        String partitionValue = null;

        if (partitionColumnPosition == BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION)
        {
            partitionValue = businessObjectDataEntity.getPartitionValue();
        }
        else
        {
            List<String> subPartitionValues = herdHelper.getSubPartitionValues(businessObjectDataEntity);

            if (partitionColumnPosition > BusinessObjectDataEntity.FIRST_PARTITION_COLUMN_POSITION && partitionColumnPosition < subPartitionValues.size() + 2)
            {
                // Please note that the value of the second partition column is located at index 0.
                partitionValue = subPartitionValues.get(partitionColumnPosition - 2);
            }
        }

        return partitionValue;
    }

    /**
     * Returns a partition filter that the specified business object data entity would match to. The filter is build as per specified sample partition filter.
     *
     * @param businessObjectDataEntity the business object data entity
     * @param samplePartitionFilter the sample partition filter
     *
     * @return the partition filter
     */
    public List<String> getPartitionFilter(BusinessObjectDataEntity businessObjectDataEntity, List<String> samplePartitionFilter)
    {
        BusinessObjectDataKey businessObjectDataKey = getBusinessObjectDataKey(businessObjectDataEntity);

        List<String> resultPartitionFilter = new ArrayList<>();

        resultPartitionFilter.add(samplePartitionFilter.get(0) != null ? businessObjectDataKey.getPartitionValue() : null);

        for (int i = 0; i < BusinessObjectDataEntity.MAX_SUBPARTITIONS; i++)
        {
            resultPartitionFilter.add(samplePartitionFilter.get(i + 1) != null ? businessObjectDataKey.getSubPartitionValues().get(i) : null);
        }

        return resultPartitionFilter;
    }

    /**
     * Gets a business object data attribute entity on the key and makes sure that it exists.
     *
     * @param businessObjectDataAttributeKey the business object data attribute key
     *
     * @return the business object data attribute entity
     * @throws ObjectNotFoundException if the business object data or the business object data attribute don't exist
     */
    public BusinessObjectDataAttributeEntity getBusinessObjectDataAttributeEntity(BusinessObjectDataAttributeKey businessObjectDataAttributeKey)
        throws ObjectNotFoundException
    {
        // Get the business object data and ensure it exists.
        BusinessObjectDataEntity businessObjectDataEntity = getBusinessObjectDataEntity(
            new BusinessObjectDataKey(businessObjectDataAttributeKey.getNamespace(), businessObjectDataAttributeKey.getBusinessObjectDefinitionName(),
                businessObjectDataAttributeKey.getBusinessObjectFormatUsage(), businessObjectDataAttributeKey.getBusinessObjectFormatFileType(),
                businessObjectDataAttributeKey.getBusinessObjectFormatVersion(), businessObjectDataAttributeKey.getPartitionValue(),
                businessObjectDataAttributeKey.getSubPartitionValues(), businessObjectDataAttributeKey.getBusinessObjectDataVersion()));

        // Load all existing business object data attribute entities into a map for quick access using lowercase attribute names.
        Map<String, BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntityMap =
            getBusinessObjectDataAttributeEntityMap(businessObjectDataEntity.getAttributes());

        // Get the relative entity using the attribute name in lowercase.
        BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity =
            businessObjectDataAttributeEntityMap.get(businessObjectDataAttributeKey.getBusinessObjectDataAttributeName().toLowerCase());
        if (businessObjectDataAttributeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Attribute with name \"%s\" does not exist for business object data {%s}.",
                businessObjectDataAttributeKey.getBusinessObjectDataAttributeName(), businessObjectDataEntityAltKeyToString(businessObjectDataEntity)));
        }

        return businessObjectDataAttributeEntity;
    }

    /**
     * Creates a map that maps business object data attribute names in lowercase to the relative business object data attribute entities.
     *
     * @param businessObjectDataAttributeEntities the collection of business object data attribute entities to be loaded into the map
     *
     * @return the map that maps business object data attribute names in lowercase to the relative business object data attribute entities
     */
    public Map<String, BusinessObjectDataAttributeEntity> getBusinessObjectDataAttributeEntityMap(
        Collection<BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntities)
    {
        Map<String, BusinessObjectDataAttributeEntity> businessObjectDataAttributeEntityMap = new HashMap<>();

        for (BusinessObjectDataAttributeEntity businessObjectDataAttributeEntity : businessObjectDataAttributeEntities)
        {
            businessObjectDataAttributeEntityMap.put(businessObjectDataAttributeEntity.getName().toLowerCase(), businessObjectDataAttributeEntity);
        }

        return businessObjectDataAttributeEntityMap;
    }

    /**
     * Gets the notification event type entity and ensure it exists.
     *
     * @param code the notification event type code (case insensitive)
     *
     * @return the notification event type entity
     * @throws ObjectNotFoundException if the entity doesn't exist
     */
    public NotificationEventTypeEntity getNotificationEventTypeEntity(String code) throws ObjectNotFoundException
    {
        NotificationEventTypeEntity notificationEventTypeEntity = herdDao.getNotificationEventTypeByCode(code);

        if (notificationEventTypeEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Notification event type with code \"%s\" doesn't exist.", code));
        }

        return notificationEventTypeEntity;
    }

    /**
     * Retrieve and ensures that a job definition entity exists.
     *
     * @param namespace the namespace (case insensitive)
     * @param jobName the job name (case insensitive)
     *
     * @return the job definition entity
     * @throws ObjectNotFoundException if the storage entity doesn't exist
     */
    public JobDefinitionEntity getJobDefinitionEntity(String namespace, String jobName) throws ObjectNotFoundException
    {
        JobDefinitionEntity jobDefinitionEntity = herdDao.getJobDefinitionByAltKey(namespace, jobName);

        if (jobDefinitionEntity == null)
        {
            throw new ObjectNotFoundException(String.format("Job definition with namespace \"%s\" and job name \"%s\" doesn't exist.", namespace, jobName));
        }

        return jobDefinitionEntity;
    }

    /**
     * Gets an EMR cluster definition entity based on the key and makes sure that it exists.
     *
     * @param emrClusterDefinitionKey the EMR cluster definition key
     *
     * @return the EMR cluster definition entity
     * @throws ObjectNotFoundException if the EMR cluster definition entity doesn't exist
     */
    public EmrClusterDefinitionEntity getEmrClusterDefinitionEntity(EmrClusterDefinitionKey emrClusterDefinitionKey) throws ObjectNotFoundException
    {
        return getEmrClusterDefinitionEntity(emrClusterDefinitionKey.getNamespace(), emrClusterDefinitionKey.getEmrClusterDefinitionName());
    }

    /**
     * Gets an EMR cluster definition entity based on the namespace and cluster definition name and makes sure that it exists.
     *
     * @param namespace the namespace
     * @param clusterDefinitionName the cluster definition name
     *
     * @return the EMR cluster definition entity
     * @throws ObjectNotFoundException if the EMR cluster definition entity doesn't exist
     */
    public EmrClusterDefinitionEntity getEmrClusterDefinitionEntity(String namespace, String clusterDefinitionName) throws ObjectNotFoundException
    {
        // Get the EMR cluster definition and ensure it exists.
        EmrClusterDefinitionEntity emrClusterDefinitionEntity = herdDao.getEmrClusterDefinitionByAltKey(namespace, clusterDefinitionName);
        if (emrClusterDefinitionEntity == null)
        {
            throw new ObjectNotFoundException("EMR cluster definition with name \"" + clusterDefinitionName + "\" doesn't exist for namespace \"" +
                namespace + "\".");
        }
        return emrClusterDefinitionEntity;
    }

    /**
     * Checks if the method name is not allowed against the configuration.
     *
     * @param methodName the method name
     *
     * @throws MethodNotAllowedException if requested method is not allowed.
     */
    public void checkNotAllowedMethod(String methodName) throws MethodNotAllowedException
    {
        boolean needToBlock = false;
        ConfigurationEntity configurationEntity = herdDao.getConfigurationByKey(ConfigurationValue.NOT_ALLOWED_HERD_ENDPOINTS.getKey());
        if (configurationEntity != null && StringUtils.isNotBlank(configurationEntity.getValueClob()))
        {
            List<String> methodsToBeBlocked = herdStringHelper.splitStringWithDefaultDelimiter(configurationEntity.getValueClob());
            needToBlock = methodsToBeBlocked.contains(methodName);
        }

        if (needToBlock)
        {
            throw new MethodNotAllowedException("The requested method is not allowed.");
        }
    }

    /**
     * Adds the JMS message to the database queue.
     *
     * @param jmsQueueName the JMS queue name
     * @param messageText the message text
     *
     * @return the JMS message entity
     */
    public JmsMessageEntity addJmsMessageToDatabaseQueue(String jmsQueueName, String messageText)
    {
        JmsMessageEntity jmsMessageEntity = new JmsMessageEntity();
        jmsMessageEntity.setJmsQueueName(jmsQueueName);
        jmsMessageEntity.setMessageText(messageText);
        jmsMessageEntity = herdDao.saveAndRefresh(jmsMessageEntity);

        // Set to schedule JMS publishing job.
        ScheduleJmsPublishingJobAdvice.setScheduleJmsPublishingJob();

        return jmsMessageEntity;
    }
}
