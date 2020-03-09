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
package org.finra.herd.app.security;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import org.finra.herd.app.AbstractAppTest;
import org.finra.herd.model.api.xml.BusinessObjectDefinition;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;

/**
 * This class tests the security user wrapper.
 */
public class SecurityUserWrapperTest extends AbstractAppTest
{
    @Test
    public void testCreateBusinessObjectDefinitionWithTrustedUser() throws Exception
    {
        // Create and persist database entities required for testing.
        namespaceDaoTestHelper.createNamespaceEntity(NAMESPACE);
        dataProviderDaoTestHelper.createDataProviderEntity(DATA_PROVIDER_NAME);

        // Override security configuration to disable the security.
        Map<String, Object> overrideMap = new HashMap<>();
        overrideMap.put(ConfigurationValue.SECURITY_ENABLED_SPEL_EXPRESSION.getKey(), "false");
        modifyPropertySourceInEnvironment(overrideMap);

        try
        {
            // Invalidate user session if exists.
            invalidateApplicationUser(null);

            // Call the relative filter to set username to trusted user in the security context.
            // This will automatically load all functions defined in the database.
            trustedUserAuthenticationFilter.init(new MockFilterConfig());
            trustedUserAuthenticationFilter.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), new MockFilterChain());

            // Create a business object definition.
            // This indirectly requires the "FN_BUSINESS_OBJECT_DEFINITIONS_POST" function point to be present in the authenticated user.
            BusinessObjectDefinitionCreateRequest request =
                new BusinessObjectDefinitionCreateRequest(NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION, BDEF_DISPLAY_NAME, NO_ATTRIBUTES);
            BusinessObjectDefinition businessObjectDefinition = businessObjectDefinitionService.createBusinessObjectDefinition(request);

            // Retrieve the newly created business object definition and validate the created by field.
            BusinessObjectDefinitionEntity businessObjectDefinitionEntity =
                businessObjectDefinitionDao.getBusinessObjectDefinitionByKey(new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME));

            // Validate the newly created entity.
            assertEquals(Long.valueOf(businessObjectDefinition.getId()), businessObjectDefinitionEntity.getId());
            String expectedUsername = TrustedApplicationUserBuilder.TRUSTED_USER_ID;
            assertEquals(expectedUsername, businessObjectDefinitionEntity.getCreatedBy());
            assertEquals(expectedUsername, businessObjectDefinitionEntity.getUpdatedBy());
        }
        finally
        {
            // Restore the property sources so we don't affect other tests.
            restorePropertySourceInEnvironment();
        }
    }
}
