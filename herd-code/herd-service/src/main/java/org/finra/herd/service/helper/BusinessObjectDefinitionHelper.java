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

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.dao.helper.JsonHelper;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionColumnKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.service.functional.QuadConsumer;

/**
 * A helper class for BusinessObjectDefinition related code.
 */
@Component
public class BusinessObjectDefinitionHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessObjectDefinitionHelper.class);

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private JsonHelper jsonHelper;

    /**
     * Returns a string representation of the business object definition key.
     *
     * @param businessObjectDefinitionKey the business object definition key
     *
     * @return the string representation of the business object definition key
     */
    public String businessObjectDefinitionKeyToString(BusinessObjectDefinitionKey businessObjectDefinitionKey)
    {
        return String.format("namespace: \"%s\", businessObjectDefinitionName: \"%s\"", businessObjectDefinitionKey.getNamespace(),
            businessObjectDefinitionKey.getBusinessObjectDefinitionName());
    }

    /**
     * Executes a function for business object definition entities.
     *
     * @param indexName the name of the index
     * @param documentType the document type
     * @param businessObjectDefinitionEntities the list of business object definitions entities
     * @param function the function to apply to all business object definitions
     */
    public void executeFunctionForBusinessObjectDefinitionEntities(final String indexName, final String documentType,
        final List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntities, final QuadConsumer<String, String, String, String> function)
    {
        // For each business object definition apply the passed in function
        businessObjectDefinitionEntities.forEach(businessObjectDefinitionEntity -> {
            // Fetch Join with .size()
            businessObjectDefinitionEntity.getAttributes().size();
            businessObjectDefinitionEntity.getBusinessObjectDefinitionTags().size();
            businessObjectDefinitionEntity.getBusinessObjectFormats().size();
            businessObjectDefinitionEntity.getColumns().size();
            businessObjectDefinitionEntity.getSampleDataFiles().size();

            // Convert the business object definition entity to a JSON string
            final String jsonString = safeObjectMapperWriteValueAsString(businessObjectDefinitionEntity);

            if (StringUtils.isNotEmpty(jsonString))
            {
                // Call the function that will process each business object definition entity against the index
                function.accept(indexName, documentType, businessObjectDefinitionEntity.getId().toString(), jsonString);
            }
        });

        LOGGER.info("Finished processing {} business object definitions with a search index function.", businessObjectDefinitionEntities.size());
    }

    /**
     * Gets a business object definition key from the specified business object definition column key.
     *
     * @param businessObjectDefinitionColumnKey the business object definition column key
     *
     * @return the business object definition key
     */
    public BusinessObjectDefinitionKey getBusinessObjectDefinitionKey(BusinessObjectDefinitionColumnKey businessObjectDefinitionColumnKey)
    {
        return new BusinessObjectDefinitionKey(businessObjectDefinitionColumnKey.getNamespace(),
            businessObjectDefinitionColumnKey.getBusinessObjectDefinitionName());
    }

    /**
     * Validates the business object definition key. This method also trims the key parameters.
     *
     * @param key the business object definition key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateBusinessObjectDefinitionKey(BusinessObjectDefinitionKey key) throws IllegalArgumentException
    {
        Assert.notNull(key, "A business object definition key must be specified.");
        key.setNamespace(alternateKeyHelper.validateStringParameter("namespace", key.getNamespace()));
        key.setBusinessObjectDefinitionName(
            alternateKeyHelper.validateStringParameter("business object definition name", key.getBusinessObjectDefinitionName()));
    }

    /**
     * Wrapper method that will safely call the object mapper write value as string method and handle the JsonProcessingException. This wrapper is needed so
     * that we can do the object mapping within a Java stream.
     *
     * @param businessObjectDefinitionEntity the business object definition entity to convert to JSON
     *
     * @return the JSON string value of the business object definition entity
     */
    private String safeObjectMapperWriteValueAsString(final BusinessObjectDefinitionEntity businessObjectDefinitionEntity)
    {
        String jsonString = "";

        try
        {
            // Convert the business object definition entity to a JSON string.
            jsonString = jsonHelper.objectToJson(businessObjectDefinitionEntity);
        }
        catch (IllegalStateException illegalStateException)
        {
            LOGGER.warn("Could not parse BusinessObjectDefinitionEntity id={" + businessObjectDefinitionEntity.getId() + "} into JSON string. ",
                illegalStateException);
        }

        return jsonString;
    }
}
