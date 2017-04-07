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
package org.finra.herd.rest;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionIndexSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionIndexSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSearchKey;
import org.finra.herd.model.api.xml.Facet;
import org.finra.herd.model.api.xml.TagKey;
import org.finra.herd.model.dto.TagIndexSearchResponseDto;
import org.finra.herd.model.dto.TagTypeIndexSearchResponseDto;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.service.BusinessObjectDefinitionService;

/**
 * This class tests search index functionality within the business object definition REST controller. This separate test class was created because this one uses
 * a mock business object definition service.
 */
public class BusinessObjectDefinitionRestControllerIndexTest extends AbstractRestTest
{
    @InjectMocks
    private BusinessObjectDefinitionRestController businessObjectDefinitionRestController;

    @Mock
    private BusinessObjectDefinitionService businessObjectDefinitionService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testIndexSearchBusinessObjectDefinitions()
    {
        // Create a new tag key with a tag type and a tag code
        TagKey tagKey = new TagKey(TAG_TYPE, TAG_CODE);

        // Create  a new business object definition search key for use in the business object definition search key list
        BusinessObjectDefinitionSearchKey businessObjectDefinitionSearchKey = new BusinessObjectDefinitionSearchKey(tagKey, INCLUDE_TAG_HIERARCHY);

        // Create a new business object definition search key list with the tag key and the include tag hierarchy boolean flag
        List<BusinessObjectDefinitionSearchKey> businessObjectDefinitionSearchKeyList = new ArrayList<>();
        businessObjectDefinitionSearchKeyList.add(businessObjectDefinitionSearchKey);

        // Create a new business object definition search filter list with the new business object definition search key list
        List<BusinessObjectDefinitionSearchFilter> businessObjectDefinitionSearchFilterList = new ArrayList<>();
        businessObjectDefinitionSearchFilterList.add(new BusinessObjectDefinitionSearchFilter(false, businessObjectDefinitionSearchKeyList));


        //Create a list of facet fields
        List<String> facetFields = new ArrayList<>();
        facetFields.add("Invalid");
        // Create a new business object definition search request that will be used when testing the index search business object definitions method
        BusinessObjectDefinitionIndexSearchRequest businessObjectDefinitionSearchRequest =
            new BusinessObjectDefinitionIndexSearchRequest(businessObjectDefinitionSearchFilterList, facetFields);

        // Create a new fields set that will be used when testing the index search business object definitions method
        Set<String> fields = Sets.newHashSet(FIELD_DATA_PROVIDER_NAME, FIELD_DISPLAY_NAME, FIELD_SHORT_DESCRIPTION);

        // Create a business object definition entity list to return from the search business object definitions by tags function
        List<BusinessObjectDefinitionEntity> businessObjectDefinitionEntityList = new ArrayList<>();
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));
        businessObjectDefinitionEntityList.add(businessObjectDefinitionDaoTestHelper
            .createBusinessObjectDefinitionEntity(NAMESPACE, BDEF_NAME_2, DATA_PROVIDER_NAME_2, BDEF_DESCRIPTION_2,
                businessObjectDefinitionServiceTestHelper.getNewAttributes()));

        // Create a list to hold the business object definitions that will be returned as part of the search response
        List<BusinessObjectDefinition> businessObjectDefinitions = new ArrayList<>();

        // Retrieve all unique business object definition entities and construct a list of business object definitions based on the requested fields.
        for (BusinessObjectDefinitionEntity businessObjectDefinitionEntity : ImmutableSet.copyOf(businessObjectDefinitionEntityList))
        {
            // Convert the business object definition entity to a business object definition and add it to the list of business object definitions that will be
            // returned as a part of the search response
            BusinessObjectDefinition businessObjectDefinition = new BusinessObjectDefinition();

            // Populate the business object definition
            businessObjectDefinition.setNamespace(businessObjectDefinitionEntity.getNamespace().getCode());
            businessObjectDefinition.setBusinessObjectDefinitionName(businessObjectDefinitionEntity.getName());
            businessObjectDefinition.setDataProviderName(businessObjectDefinitionEntity.getDataProvider().getName());
            businessObjectDefinition.setShortDescription(StringUtils.left(businessObjectDefinitionEntity.getDescription(), SHORT_DESCRIPTION_LENGTH));
            businessObjectDefinition.setDisplayName(businessObjectDefinitionEntity.getDisplayName());
            businessObjectDefinitions.add(businessObjectDefinition);
        }

        List<TagTypeIndexSearchResponseDto> tagTypeIndexSearchResponseDtos = new ArrayList<>();
        List<TagIndexSearchResponseDto> tagIndexSearchResponseDtos = new ArrayList<>();
        tagIndexSearchResponseDtos.add(new TagIndexSearchResponseDto(TAG_CODE, TAG_COUNT, TAG_DISPLAY_NAME));
        tagIndexSearchResponseDtos.add(new TagIndexSearchResponseDto(TAG_CODE_2, TAG_COUNT, TAG_DISPLAY_NAME_2));
        TagTypeIndexSearchResponseDto tagTypeIndexSearchResponseDto =
            new TagTypeIndexSearchResponseDto(TAG_TYPE, TAG_TYPE_COUNT, tagIndexSearchResponseDtos, TAG_TYPE_DISPLAY_NAME);
        tagTypeIndexSearchResponseDtos.add(tagTypeIndexSearchResponseDto);

        List<Facet> tagTypeFacets = new ArrayList<>();
        for (TagTypeIndexSearchResponseDto tagTypeIndexSearchResponse : ImmutableSet.copyOf(tagTypeIndexSearchResponseDtos))
        {

            List<Facet> tagFacets = new ArrayList<>();

            for (TagIndexSearchResponseDto tagIndexSearchResponseDto : tagTypeIndexSearchResponse.getTagIndexSearchResponseDtos())
            {
                Facet tagFacet =
                    new Facet(tagIndexSearchResponseDto.getTagDisplayName(), tagIndexSearchResponseDto.getCount(), TagIndexSearchResponseDto.getFacetType(),
                        tagIndexSearchResponseDto.getTagCode(), null);
                tagFacets.add(tagFacet);
            }

            tagTypeFacets.add(
                new Facet(tagTypeIndexSearchResponse.getDisplayName(), tagTypeIndexSearchResponse.getCount(), TagTypeIndexSearchResponseDto.getFacetType(),
                    tagTypeIndexSearchResponse.getCode(), tagFacets));
        }

        // Construct business object search response.
        BusinessObjectDefinitionIndexSearchResponse businessObjectDefinitionSearchResponse = new BusinessObjectDefinitionIndexSearchResponse();
        businessObjectDefinitionSearchResponse.setBusinessObjectDefinitions(businessObjectDefinitions);
        businessObjectDefinitionSearchResponse.setFacets(tagTypeFacets);

        // Mock the call to the business object definition service
        when(businessObjectDefinitionService.indexSearchBusinessObjectDefinitions(businessObjectDefinitionSearchRequest, fields))
            .thenReturn(businessObjectDefinitionSearchResponse);

        // Create a business object definition.
        BusinessObjectDefinitionIndexSearchResponse businessObjectDefinitionSearchResponseFromRestCall =
            businessObjectDefinitionRestController.indexSearchBusinessObjectDefinitions(fields, businessObjectDefinitionSearchRequest);

        // Verify the method call to businessObjectDefinitionService.indexAllBusinessObjectDefinitions()
        verify(businessObjectDefinitionService, times(1)).indexSearchBusinessObjectDefinitions(businessObjectDefinitionSearchRequest, fields);

        // Validate the returned object.
        assertThat("Business object definition index search response was null.", businessObjectDefinitionSearchResponseFromRestCall, not(nullValue()));
        assertThat("Business object definition index search response was not correct.", businessObjectDefinitionSearchResponseFromRestCall,
            is(businessObjectDefinitionSearchResponse));
        assertThat("Business object definition index search response was not an instance of BusinessObjectDefinitionSearchResponse.class.",
            businessObjectDefinitionSearchResponseFromRestCall, instanceOf(BusinessObjectDefinitionIndexSearchResponse.class));
    }
}
