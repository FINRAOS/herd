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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class BusinessObjectDefinitionDescriptionSuggestionStatusDaoTest extends AbstractDaoTest
{
    @Test
    public void testGetSearchIndexStatusByCode()
    {
        // Create database entities required for testing.
        businessObjectDefinitionDescriptionSuggestionStatusDaoTestHelper
            .createBusinessObjectDefinitionDescriptionSuggestionStatusEntity(BDEF_DESCRIPTION_SUGGESTION_STATUS);
        businessObjectDefinitionDescriptionSuggestionStatusDaoTestHelper
            .createBusinessObjectDefinitionDescriptionSuggestionStatusEntity(BDEF_DESCRIPTION_SUGGESTION_STATUS_2);

        // Retrieve the relative business object definition description suggestion status entities and validate the results.
        assertEquals(BDEF_DESCRIPTION_SUGGESTION_STATUS, businessObjectDefinitionDescriptionSuggestionStatusDao
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS).getCode());
        assertEquals(BDEF_DESCRIPTION_SUGGESTION_STATUS_2, businessObjectDefinitionDescriptionSuggestionStatusDao
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS_2).getCode());

        // Test case insensitivity for the business object definition description suggestion status code.
        assertEquals(BDEF_DESCRIPTION_SUGGESTION_STATUS, businessObjectDefinitionDescriptionSuggestionStatusDao
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS.toUpperCase()).getCode());
        assertEquals(BDEF_DESCRIPTION_SUGGESTION_STATUS, businessObjectDefinitionDescriptionSuggestionStatusDao
            .getBusinessObjectDefinitionDescriptionSuggestionStatusByCode(BDEF_DESCRIPTION_SUGGESTION_STATUS.toLowerCase()).getCode());

        // Confirm negative results when using non-existing business object definition description suggestion status code.
        assertNull(businessObjectDefinitionDescriptionSuggestionStatusDao.getBusinessObjectDefinitionDescriptionSuggestionStatusByCode("I_DO_NOT_EXIST"));
    }
}
