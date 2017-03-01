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

import java.util.Set;

import org.finra.herd.model.api.xml.IndexSearchRequest;
import org.finra.herd.model.api.xml.IndexSearchResponse;

/**
 * IndexSearchDao
 */
public interface IndexSearchDao
{
    String TAG_CODE_SEARCH_FIELD = "tagCode";

    Float TAG_CODE_SEARCH_BOOST = 15f;

    String TAG_TYPE_CODE_SEARCH_FIELD = "tagType.code";

    String TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD = "tagType.displayName";

    String TAG_TYPE_DESCRIPTION_SEARCH_FIELD = "tagType.description";

    String DISPLAY_NAME_SEARCH_FIELD = "displayName";

    Float DISPLAY_NAME_SEARCH_BOOST = 10f;

    String DESCRIPTION_SEARCH_FIELD = "description";

    Float DESCRIPTION_SEARCH_BOOST = 10f;

    String CHILDREN_TAG_ENTITIES_TAG_CODE_SEARCH_FIELD = "childrenTagEntities.tagCode";

    String CHILDREN_TAG_ENTITIES_TAG_TYPE_CODE_SEARCH_FIELD = "childrenTagEntities.tagType.code";

    String CHILDREN_TAG_ENTITIES_TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD = "childrenTagEntities.tagType.displayName";

    String CHILDREN_TAG_ENTITIES_TAG_TYPE_DESCRIPTION_SEARCH_FIELD = "childrenTagEntities.tagType.description";

    String CHILDREN_TAG_ENTITIES_DISPLAY_NAME_SEARCH_FIELD = "childrenTagEntities.displayName";

    String CHILDREN_TAG_ENTITIES_DESCRIPTION_SEARCH_FIELD = "childrenTagEntities.description";

    String NAME_SEARCH_FIELD = "name";

    Float NAME_SEARCH_BOOST = 15f;

    String DATA_PROVIDER_SEARCH_FIELD = "dataProvider.name";

    Float DATA_PROVIDER_SEARCH_BOOST = 10f;

    String NAMESPACE_CODE_SEARCH_FIELD = "namespace.code";

    Float NAMESPACE_CODE_SEARCH_BOOST = 15f;

    String BUSINESS_OBJECTS_FORMATS_USAGE_SEARCH_FIELD = "businessObjectFormats.usage";

    String BUSINESS_OBJECTS_FORMATS_FILE_TYPE_CODE_SEARCH_FIELD = "businessObjectFormats.fileType.code";

    String BUSINESS_OBJECTS_FORMATS_FILE_TYPE_DESCRIPTION_SEARCH_FIELD = "businessObjectFormats.fileType.description";

    String BUSINESS_OBJECTS_FORMATS_DESCRIPTION_SEARCH_FIELD = "businessObjectFormats.description";

    String BUSINESS_OBJECTS_FORMATS_ATTRIBUTES_NAME_SEARCH_FIELD = "businessObjectFormats.attributes.name";

    String BUSINESS_OBJECTS_FORMATS_ATTRIBUTES_VALUE_SEARCH_FIELD = "businessObjectFormats.attributes.value";

    String BUSINESS_OBJECTS_FORMATS_PARTITION_KEY_SEARCH_FIELD = "businessObjectFormats.partitionKey";

    String BUSINESS_OBJECTS_FORMATS_PARTITION__SEARCH_FIELD = "businessObjectFormats.partitionKeyGroup.partitionKeyGroupName";

    String BUSINESS_OBJECTS_FORMATS_ATTRIBUTE_DEFINITIONS_NAME_SEARCH_FIELD = "businessObjectFormats.attributeDefinitions.name";

    String ATTRIBUTES_NAME_SEARCH_FIELD = "attributes.name";

    String ATTRIBUTES_VALUE_SEARCH_FIELD = "attributes.value";

    String COLUMNS_NAME_SEARCH_FIELD = "columns.name";

    Float COLUMNS_NAME_SEARCH_BOOST = 10f;

    String COLUMNS_DESCRIPTION_SEARCH_FIELD = "columns.description";

    Float COLUMNS_DESCRIPTION_SEARCH_BOOST = 10f;

    String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_TAG_CODE_SEARCH_FIELD = "businessObjectDefinitionTags.tag.tagCode";

    String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_TAG_TYPE_CODE_SEARCH_FIELD = "businessObjectDefinitionTags.tag.tagType.code";

    String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD = "businessObjectDefinitionTags.tag.tagType.displayName";

    String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_DISPLAY_NAME_SEARCH_FIELD = "businessObjectDefinitionTags.tag.displayName";

    String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_DESCRIPTION_SEARCH_FIELD = "businessObjectDefinitionTags.tag.description";

    String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_TAG_CODE_SEARCH_FIELD = "businessObjectDefinitionTags.tag.childrenTagEntities.tagCode";

    String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_TAG_TYPE_CODE_SEARCH_FIELD =

        "businessObjectDefinitionTags.tag.childrenTagEntities.tagType.code";

    String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_TAG_TYPE_DISPLAY_NAME_SEARCH_FIELD =

        "businessObjectDefinitionTags.tag.childrenTagEntities.tagType.displayName";

    String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_DISPLAY_NAME_SEARCH_FIELD =

        "businessObjectDefinitionTags.tag.childrenTagEntities.displayName";

    String BUSINESS_OBJECT_DEFINITION_TAGS_TAG_CHILDREN_TAG_ENTITIES_DESCRIPTION_SEARCH_FIELD =

        "businessObjectDefinitionTags.tag.childrenTagEntities.description";

    String DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_USAGE_SEARCH_FIELD = "descriptiveBusinessObjectFormat.usage";

    String DESCRIPTIVE_BUSINESS_OBJECT_FORMAT_FILE_TYPE_CODE_SEARCH_FIELD = "descriptiveBusinessObjectFormat.fileType.code";

    String SAMPLE_DATA_FILES_FILE_NAME_SEARCH_FIELD = "sampleDataFiles.fileName";

    String SAMPLE_DATA_FILES_DIRECTORY_PATH_SEARCH_FIELD = "sampleDataFiles.directoryPath";

    /**
     * The index search method will accept an index search request as a parameter and will return an index search result. The result will be an index search
     * based on the search term contained in the index search request.
     *
     * @param request the index search request containing the search term
     * @param fields the set of fields that are to be returned in the index indexSearch response
     *
     * @return the index search response containing the search results
     */
    IndexSearchResponse indexSearch(final IndexSearchRequest request, final Set<String> fields);
}
