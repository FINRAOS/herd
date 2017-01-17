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
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.JmsMessage;
import org.finra.herd.model.dto.SearchIndexUpdateDto;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;

/**
 * SearchIndexUpdateHelper class contains helper methods needed to process a search index update.
 */
@Component
public class SearchIndexUpdateHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchIndexUpdateHelper.class);

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private JmsMessageInMemoryQueue jmsMessageInMemoryQueue;

    @Autowired
    private JsonHelper jsonHelper;

    /**
     * Modify a business object definition
     *
     * @param businessObjectDefinitionEntity the business object definition entity to modify
     * @param modificationType the type of modification
     */
    public void modifyBusinessObjectDefinitionInSearchIndex(BusinessObjectDefinitionEntity businessObjectDefinitionEntity, String modificationType)
    {
        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId());
        processMessage(jsonHelper.objectToJson(new SearchIndexUpdateDto(businessObjectDefinitionIds, modificationType)));
    }

    /**
     * Modify a list of business object definitions
     *
     * @param businessObjectDefinitionEntityList the business object definition entities to modify
     * @param modificationType the type of modification
     */
    public void modifyBusinessObjectDefinitionsInSearchIndex(List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList, String modificationType)
    {
        List<Integer> businessObjectDefinitionIds = new ArrayList<>();
        businessObjectDefinitionEntityList.forEach(businessObjectDefinitionEntity -> businessObjectDefinitionIds.add(businessObjectDefinitionEntity.getId()));
        processMessage(jsonHelper.objectToJson(new SearchIndexUpdateDto(businessObjectDefinitionIds, modificationType)));
    }

    /**
     * Returns the SQS queue name. Throws {@link IllegalStateException} if SQS queue name is undefined.
     *
     * @return the sqs queue name
     */
    private String getSqsQueueName()
    {
        String sqsQueueName = configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME);

        if (StringUtils.isBlank(sqsQueueName))
        {
            throw new IllegalStateException(String.format("SQS queue name not found. Ensure the \"%s\" configuration entry is configured.",
                ConfigurationValue.SEARCH_INDEX_UPDATE_SQS_QUEUE_NAME.getKey()));
        }

        return sqsQueueName;
    }

    /**
     * Processes a message by adding it to the database "queue" table to ultimately be placed on the real queue by a separate job.
     *
     * @param messageText the message text to place on the queue
     */
    private void processMessage(String messageText)
    {
        boolean isSearchIndexUpdateSqsNotificationEnabled =
            Boolean.valueOf(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED));

        LOGGER.debug(String.format("searchIndexUpdateSqsNotificationEnabled: %s, messageText:%n%s", isSearchIndexUpdateSqsNotificationEnabled, messageText));

        // Only process messages if the service is enabled.
        if (isSearchIndexUpdateSqsNotificationEnabled)
        {
            // Add the message to the database queue if a message was configured. Otherwise, log a warning.
            if (StringUtils.isBlank(messageText))
            {
                LOGGER.warn("Not sending search index update message because it is not configured.");
            }
            else
            {
                // Add the JMS message to the "in-memory" JMS message queue to be published by the advice.
                jmsMessageInMemoryQueue.add(new JmsMessage(getSqsQueueName(), messageText));
            }
        }
    }
}
