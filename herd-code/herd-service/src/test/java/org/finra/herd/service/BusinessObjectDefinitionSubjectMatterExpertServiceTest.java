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
package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.ObjectNotFoundException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpert;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionSubjectMatterExpertKeys;
import org.finra.herd.model.jpa.BusinessObjectDefinitionSubjectMatterExpertEntity;

/**
 * This class tests various functionality within the business object definition subject matter expert service.
 */
public class BusinessObjectDefinitionSubjectMatterExpertServiceTest extends AbstractServiceTest
{
    @Test
    public void testCreateBusinessObjectDefinitionSubjectMatterExpert()
    {
        // Create a business object definition subject matter expert key.
        BusinessObjectDefinitionSubjectMatterExpertKey key = new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create and persist the relative database entities.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, DESCRIPTION);

        // Create a business object definition subject matter expert.
        BusinessObjectDefinitionSubjectMatterExpert resultBusinessObjectDefinitionSubjectMatterExpert = businessObjectDefinitionSubjectMatterExpertService
            .createBusinessObjectDefinitionSubjectMatterExpert(new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(key));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpert(resultBusinessObjectDefinitionSubjectMatterExpert.getId(), key),
            resultBusinessObjectDefinitionSubjectMatterExpert);
    }

    @Test
    public void testCreateBusinessObjectDefinitionSubjectMatterExpertBusinessObjectDefinitionNoExists()
    {
        // Try to create a business object definition subject matter expert for a non-existing business object definition.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService.createBusinessObjectDefinitionSubjectMatterExpert(
                new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(
                    new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID)));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME),
                e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionSubjectMatterExpertBusinessObjectDefinitionSubjectMatterExpertAlreadyExists()
    {
        // Create and persist a business object definition subject matter expert entity.
        businessObjectDefinitionSubjectMatterExpertDaoTestHelper
            .createBusinessObjectDefinitionSubjectMatterExpertEntity(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID));

        // Try to add a duplicate business object definition subject matter expert.
        for (String userId : Arrays.asList(USER_ID, USER_ID.toUpperCase(), USER_ID.toLowerCase()))
        {
            try
            {
                businessObjectDefinitionSubjectMatterExpertService.createBusinessObjectDefinitionSubjectMatterExpert(
                    new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(
                        new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, userId)));
                fail();
            }
            catch (AlreadyExistsException e)
            {
                assertEquals(String.format("Unable to create business object definition subject matter expert with user id \"%s\" " +
                    "because it already exists for the business object definition {%s}.", userId,
                    businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionKeyAsString(BDEF_NAMESPACE, BDEF_NAME)), e.getMessage());
            }
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionSubjectMatterExpertInvalidParameters()
    {
        // Try to create a business object definition subject matter expert when business object definition namespace contains a forward slash character.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService.createBusinessObjectDefinitionSubjectMatterExpert(
                new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(
                    new BusinessObjectDefinitionSubjectMatterExpertKey(addSlash(BDEF_NAMESPACE), BDEF_NAME, USER_ID)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Namespace can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object definition subject matter expert when business object definition name contains a forward slash character.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService.createBusinessObjectDefinitionSubjectMatterExpert(
                new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(
                    new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, addSlash(BDEF_NAME), USER_ID)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition name can not contain a forward slash character.", e.getMessage());
        }

        // Try to create a business object definition subject matter expert when user id contains a forward slash character.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService.createBusinessObjectDefinitionSubjectMatterExpert(
                new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(
                    new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, addSlash(USER_ID))));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Business object definition subject matter expert name can not contain a forward slash character.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionSubjectMatterExpertLowerCaseParameters()
    {
        // Create and persist the relative database entities.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, DESCRIPTION);

        // Create a business object definition subject matter expert using lower case parameter values.
        BusinessObjectDefinitionSubjectMatterExpert resultBusinessObjectDefinitionSubjectMatterExpert = businessObjectDefinitionSubjectMatterExpertService
            .createBusinessObjectDefinitionSubjectMatterExpert(new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(
                new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), USER_ID.toLowerCase())));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpert(resultBusinessObjectDefinitionSubjectMatterExpert.getId(),
            new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID.toLowerCase())),
            resultBusinessObjectDefinitionSubjectMatterExpert);
    }

    @Test
    public void testCreateBusinessObjectDefinitionSubjectMatterExpertMissingRequiredParameters()
    {
        // Try to create a business object definition subject matter expert when business object definition namespace is not specified.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService.createBusinessObjectDefinitionSubjectMatterExpert(
                new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(
                    new BusinessObjectDefinitionSubjectMatterExpertKey(BLANK_TEXT, BDEF_NAME, USER_ID)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to create a business object definition subject matter expert when business object definition name is not specified.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService.createBusinessObjectDefinitionSubjectMatterExpert(
                new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(
                    new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BLANK_TEXT, USER_ID)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to create a business object definition subject matter expert when user id is not specified.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService.createBusinessObjectDefinitionSubjectMatterExpert(
                new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(
                    new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, BLANK_TEXT)));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A user id must be specified.", e.getMessage());
        }
    }

    @Test
    public void testCreateBusinessObjectDefinitionSubjectMatterExpertTrimParameters()
    {
        // Create and persist the relative database entities.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, DESCRIPTION);

        // Create a business object definition subject matter expert using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinitionSubjectMatterExpert resultBusinessObjectDefinitionSubjectMatterExpert = businessObjectDefinitionSubjectMatterExpertService
            .createBusinessObjectDefinitionSubjectMatterExpert(new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(
                new BusinessObjectDefinitionSubjectMatterExpertKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME.toUpperCase()),
                    addWhitespace(USER_ID))));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpert(resultBusinessObjectDefinitionSubjectMatterExpert.getId(),
            new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID)), resultBusinessObjectDefinitionSubjectMatterExpert);
    }

    @Test
    public void testCreateBusinessObjectDefinitionSubjectMatterExpertUpperCaseParameters()
    {
        // Create and persist the relative database entities.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, DESCRIPTION);

        // Create a business object definition subject matter expert using upper case parameter values.
        BusinessObjectDefinitionSubjectMatterExpert resultBusinessObjectDefinitionSubjectMatterExpert = businessObjectDefinitionSubjectMatterExpertService
            .createBusinessObjectDefinitionSubjectMatterExpert(new BusinessObjectDefinitionSubjectMatterExpertCreateRequest(
                new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), USER_ID.toUpperCase())));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpert(resultBusinessObjectDefinitionSubjectMatterExpert.getId(),
            new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID.toUpperCase())),
            resultBusinessObjectDefinitionSubjectMatterExpert);
    }

    @Test
    public void testDeleteBusinessObjectDefinitionSubjectMatterExpert()
    {
        // Create a business object definition subject matter expert key.
        BusinessObjectDefinitionSubjectMatterExpertKey key = new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create and persist the relative database entities.
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity =
            businessObjectDefinitionSubjectMatterExpertDaoTestHelper.createBusinessObjectDefinitionSubjectMatterExpertEntity(key);

        // Validate that this business object definition subject matter expert exists.
        assertNotNull(businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(key));

        // Delete this business object definition subject matter expert.
        BusinessObjectDefinitionSubjectMatterExpert deletedBusinessObjectDefinitionSubjectMatterExpert =
            businessObjectDefinitionSubjectMatterExpertService.deleteBusinessObjectDefinitionSubjectMatterExpert(key);

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpert(businessObjectDefinitionSubjectMatterExpertEntity.getId(), key),
            deletedBusinessObjectDefinitionSubjectMatterExpert);

        // Ensure that this business object definition subject matter expert is no longer there.
        assertNull(businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(key));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionSubjectMatterExpertBusinessObjectDefinitionSubjectMatterExpertNoExists()
    {
        // Try to delete a non-existing business object definition subject matter expert.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService
                .deleteBusinessObjectDefinitionSubjectMatterExpert(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(String.format("Subject matter expert with user id \"%s\" does not exist for business object definition {%s}.", USER_ID,
                businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionKeyAsString(BDEF_NAMESPACE, BDEF_NAME)), e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDefinitionSubjectMatterExpertLowerCaseParameters()
    {
        // Create a business object definition subject matter expert key.
        BusinessObjectDefinitionSubjectMatterExpertKey key = new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create and persist a business object definition subject matter expert.
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity =
            businessObjectDefinitionSubjectMatterExpertDaoTestHelper.createBusinessObjectDefinitionSubjectMatterExpertEntity(key);

        // Validate that this business object definition subject matter expert exists.
        assertNotNull(businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(key));

        // Delete this business object definition subject matter expert using lower case parameter values.
        BusinessObjectDefinitionSubjectMatterExpert deletedBusinessObjectDefinitionSubjectMatterExpert = businessObjectDefinitionSubjectMatterExpertService
            .deleteBusinessObjectDefinitionSubjectMatterExpert(
                new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase(), USER_ID.toLowerCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpert(businessObjectDefinitionSubjectMatterExpertEntity.getId(), key),
            deletedBusinessObjectDefinitionSubjectMatterExpert);

        // Ensure that this business object definition subject matter expert is no longer there.
        assertNull(businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(key));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionSubjectMatterExpertMissingRequiredParameters()
    {
        // Try to delete a business object definition subject matter expert when business object definition namespace is not specified.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService
                .deleteBusinessObjectDefinitionSubjectMatterExpert(new BusinessObjectDefinitionSubjectMatterExpertKey(BLANK_TEXT, BDEF_NAME, USER_ID));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to delete a business object definition subject matter expert when business object definition name is not specified.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService
                .deleteBusinessObjectDefinitionSubjectMatterExpert(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BLANK_TEXT, USER_ID));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }

        // Try to delete a business object definition subject matter expert when user id is not specified.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService
                .deleteBusinessObjectDefinitionSubjectMatterExpert(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, BLANK_TEXT));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A user id must be specified.", e.getMessage());
        }
    }

    @Test
    public void testDeleteBusinessObjectDefinitionSubjectMatterExpertTrimParameters()
    {
        // Create a business object definition subject matter expert key.
        BusinessObjectDefinitionSubjectMatterExpertKey key = new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create and persist a business object definition subject matter expert.
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity =
            businessObjectDefinitionSubjectMatterExpertDaoTestHelper.createBusinessObjectDefinitionSubjectMatterExpertEntity(key);

        // Validate that this business object definition subject matter expert exists.
        assertNotNull(businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(key));

        // Delete this business object definition subject matter expert using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinitionSubjectMatterExpert deletedBusinessObjectDefinitionSubjectMatterExpert = businessObjectDefinitionSubjectMatterExpertService
            .deleteBusinessObjectDefinitionSubjectMatterExpert(
                new BusinessObjectDefinitionSubjectMatterExpertKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME), addWhitespace(USER_ID)));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpert(businessObjectDefinitionSubjectMatterExpertEntity.getId(), key),
            deletedBusinessObjectDefinitionSubjectMatterExpert);

        // Ensure that this business object definition subject matter expert is no longer there.
        assertNull(businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(key));
    }

    @Test
    public void testDeleteBusinessObjectDefinitionSubjectMatterExpertUpperCaseParameters()
    {
        // Create a business object definition subject matter expert key.
        BusinessObjectDefinitionSubjectMatterExpertKey key = new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID);

        // Create and persist a business object definition subject matter expert.
        BusinessObjectDefinitionSubjectMatterExpertEntity businessObjectDefinitionSubjectMatterExpertEntity =
            businessObjectDefinitionSubjectMatterExpertDaoTestHelper.createBusinessObjectDefinitionSubjectMatterExpertEntity(key);

        // Validate that this business object definition subject matter expert exists.
        assertNotNull(businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(key));

        // Delete this business object definition subject matter expert using upper case parameter values.
        BusinessObjectDefinitionSubjectMatterExpert deletedBusinessObjectDefinitionSubjectMatterExpert = businessObjectDefinitionSubjectMatterExpertService
            .deleteBusinessObjectDefinitionSubjectMatterExpert(
                new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase(), USER_ID.toUpperCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpert(businessObjectDefinitionSubjectMatterExpertEntity.getId(), key),
            deletedBusinessObjectDefinitionSubjectMatterExpert);

        // Ensure that this business object definition subject matter expert is no longer there.
        assertNull(businessObjectDefinitionSubjectMatterExpertDao.getBusinessObjectDefinitionSubjectMatterExpertByKey(key));
    }

    @Test
    public void testGetBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition() throws Exception
    {
        // Create business object definition subject matter expert keys. The keys are listed out of order to validate the sorting.
        List<BusinessObjectDefinitionSubjectMatterExpertKey> keys = Arrays
            .asList(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID_2),
                new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID));

        // Create and persist the relative database entities.
        for (BusinessObjectDefinitionSubjectMatterExpertKey key : keys)
        {
            businessObjectDefinitionSubjectMatterExpertDaoTestHelper.createBusinessObjectDefinitionSubjectMatterExpertEntity(key);
        }

        // Get a list of business object definition subject matter expert keys for the specified business object definition.
        BusinessObjectDefinitionSubjectMatterExpertKeys resultBusinessObjectDefinitionSubjectMatterExperts = businessObjectDefinitionSubjectMatterExpertService
            .getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpertKeys(Arrays.asList(keys.get(1), keys.get(0))),
            resultBusinessObjectDefinitionSubjectMatterExperts);
    }

    @Test
    public void testGetBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinitionBusinessObjectDefinitionNoExists()
    {
        // Try to get a list of business object definition subject matter expert keys when business object definition does not exist.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService
                .getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME));
            fail();
        }
        catch (ObjectNotFoundException e)
        {
            assertEquals(businessObjectDefinitionServiceTestHelper.getExpectedBusinessObjectDefinitionNotFoundErrorMessage(BDEF_NAMESPACE, BDEF_NAME),
                e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinitionLowerCaseParameters()
    {
        // Create and persist business object definition subject matter expert entities.
        businessObjectDefinitionSubjectMatterExpertDaoTestHelper
            .createBusinessObjectDefinitionSubjectMatterExpertEntity(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID_2));
        businessObjectDefinitionSubjectMatterExpertDaoTestHelper
            .createBusinessObjectDefinitionSubjectMatterExpertEntity(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID));

        // Get a list of business object definition subject matter expert keys using lower case parameter values.
        BusinessObjectDefinitionSubjectMatterExpertKeys resultBusinessObjectDefinitionSubjectMatterExpertKeys =
            businessObjectDefinitionSubjectMatterExpertService.getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(
                new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toLowerCase(), BDEF_NAME.toLowerCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpertKeys(Arrays
            .asList(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID),
                new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID_2))),
            resultBusinessObjectDefinitionSubjectMatterExpertKeys);
    }

    @Test
    public void testGetBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinitionMissingRequiredParameters()
    {
        // Try to get a list of business object definition subject matter expert keys when business object definition namespace is not specified.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService
                .getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(new BusinessObjectDefinitionKey(BLANK_TEXT, BDEF_NAME));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A namespace must be specified.", e.getMessage());
        }

        // Try to get a list of business object definition subject matter expert keys when business object definition name is not specified.
        try
        {
            businessObjectDefinitionSubjectMatterExpertService
                .getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BLANK_TEXT));
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A business object definition name must be specified.", e.getMessage());
        }
    }

    @Test
    public void testGetBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinitionSubjectMatterExpertsNoExist()
    {
        // Create and persist a business object definition entity.
        businessObjectDefinitionDaoTestHelper.createBusinessObjectDefinitionEntity(BDEF_NAMESPACE, BDEF_NAME, DATA_PROVIDER_NAME, BDEF_DESCRIPTION);

        // Try to get a list of business object definition subject matter expert keys when business object definition subject matter experts do not exist.
        assertEquals(0, businessObjectDefinitionSubjectMatterExpertService
            .getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(new BusinessObjectDefinitionKey(BDEF_NAMESPACE, BDEF_NAME))
            .getBusinessObjectDefinitionSubjectMatterExpertKeys().size());
    }

    @Test
    public void testGetBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinitionTrimParameters()
    {
        // Create and persist business object definition subject matter expert entities.
        businessObjectDefinitionSubjectMatterExpertDaoTestHelper
            .createBusinessObjectDefinitionSubjectMatterExpertEntity(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID_2));
        businessObjectDefinitionSubjectMatterExpertDaoTestHelper
            .createBusinessObjectDefinitionSubjectMatterExpertEntity(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID));

        // Get a list of business object definition subject matter expert keys using input parameters with leading and trailing empty spaces.
        BusinessObjectDefinitionSubjectMatterExpertKeys resultBusinessObjectDefinitionSubjectMatterExpertKeys =
            businessObjectDefinitionSubjectMatterExpertService.getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(
                new BusinessObjectDefinitionKey(addWhitespace(BDEF_NAMESPACE), addWhitespace(BDEF_NAME)));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpertKeys(Arrays
            .asList(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID),
                new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID_2))),
            resultBusinessObjectDefinitionSubjectMatterExpertKeys);
    }

    @Test
    public void testGetBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinitionUpperCaseParameters()
    {
        // Create and persist business object definition subject matter expert entities.
        businessObjectDefinitionSubjectMatterExpertDaoTestHelper
            .createBusinessObjectDefinitionSubjectMatterExpertEntity(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID_2));
        businessObjectDefinitionSubjectMatterExpertDaoTestHelper
            .createBusinessObjectDefinitionSubjectMatterExpertEntity(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID));

        // Get a list of business object definition subject matter expert keys using upper case parameter values.
        BusinessObjectDefinitionSubjectMatterExpertKeys resultBusinessObjectDefinitionSubjectMatterExpertKeys =
            businessObjectDefinitionSubjectMatterExpertService.getBusinessObjectDefinitionSubjectMatterExpertsByBusinessObjectDefinition(
                new BusinessObjectDefinitionKey(BDEF_NAMESPACE.toUpperCase(), BDEF_NAME.toUpperCase()));

        // Validate the returned object.
        assertEquals(new BusinessObjectDefinitionSubjectMatterExpertKeys(Arrays
            .asList(new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID),
                new BusinessObjectDefinitionSubjectMatterExpertKey(BDEF_NAMESPACE, BDEF_NAME, USER_ID_2))),
            resultBusinessObjectDefinitionSubjectMatterExpertKeys);
    }
}
