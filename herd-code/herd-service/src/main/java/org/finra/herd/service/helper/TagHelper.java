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
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.jpa.TagEntity;
import org.finra.herd.service.functional.QuadConsumer;

/**
 * A helper class for Tag related code
 */
@Component
public class TagHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TagHelper.class);

    @Autowired
    private AlternateKeyHelper alternateKeyHelper;

    @Autowired
    private JsonHelper jsonHelper;

    /**
     * Executes a function for tag entities.
     *
     * @param indexName the name of the index
     * @param documentType the document type
     * @param tagEntities the list of tag entities
     * @param function the function to apply to all tags
     */
    public void executeFunctionForTagEntities(final String indexName, final String documentType, final List<TagEntity> tagEntities,
        final QuadConsumer<String, String, String, String> function)
    {
        // For each tag apply the passed in function
        tagEntities.forEach(tagEntity -> {
            // Fetch Join with .size()
            tagEntity.getChildrenTagEntities().size();

            // Convert the tag entity to a JSON string
            final String jsonString = safeObjectMapperWriteValueAsString(tagEntity);

            if (StringUtils.isNotEmpty(jsonString))
            {
                // Call the function that will process each tag entity against the index
                try
                {
                    function.accept(indexName, documentType, tagEntity.getId().toString(), jsonString);
                }
                catch(Exception ex)
                {
                    LOGGER.warn("Index operation exception is logged {} for {}, {}, {}, {}", ex, indexName, documentType,
                        tagEntity.getId().toString(), jsonString);
                }
            }
        });

        LOGGER.info("Finished processing {} tags with a search index function.", tagEntities.size());
    }

    /**
     * Wrapper method that will safely call the object mapper write value as string method and handle the JsonProcessingException. This wrapper is needed so
     * that we can do the object mapping within a Java stream.
     *
     * @param tagEntity the entity to convert to JSON
     *
     * @return JSON string value of the object
     */
    public String safeObjectMapperWriteValueAsString(final TagEntity tagEntity)
    {
        String jsonString = "";

        try
        {
            // Convert the tag entity to a JSON string
            jsonString = jsonHelper.objectToJson(tagEntity);
        }
        catch (IllegalStateException illegalStateException)
        {
            LOGGER.warn("Could not parse tagEntity id={" + tagEntity.getId() + "} into JSON string. ", illegalStateException);
        }

        return jsonString;
    }

    /**
     * Validates a tag key. This method also trims the key parameters.
     *
     * @param tagKey the tag key
     *
     * @throws IllegalArgumentException if any validation errors were found
     */
    public void validateTagKey(TagKey tagKey) throws IllegalArgumentException
    {
        Assert.notNull(tagKey, "A tag key must be specified.");
        tagKey.setTagTypeCode(alternateKeyHelper.validateStringParameter("tag type code", tagKey.getTagTypeCode()));
        tagKey.setTagCode(alternateKeyHelper.validateStringParameter("tag code", tagKey.getTagCode()));
    }
}
