package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.GlobalAttributeDefinitionDao;
import org.finra.herd.model.api.xml.GlobalAttributeDefinition;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionCreateRequest;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKeys;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionEntity;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.GlobalAttributeDefinitionDaoHelper;
import org.finra.herd.service.helper.GlobalAttributeDefinitionHelper;
import org.finra.herd.service.impl.GlobalAttributeDefinitionServiceImpl;

/**
 * This class tests the functionality of global attribute definition service
 */
public class GlobalAttributeDefinitionServiceTest extends AbstractServiceTest
{
    @InjectMocks
    private GlobalAttributeDefinitionServiceImpl globalAttributeDefinitionService;

    @Mock
    private GlobalAttributeDefinitionDao globalAttributeDefinitionDao;

    @Mock
    private GlobalAttributeDefinitionHelper globalAttributeDefinitionHelper;

    @Mock
    private GlobalAttributeDefinitionDaoHelper globalAttributeDefinitionDaoHelper;

    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateGlobalAttributeDefinition()
    {

        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        GlobalAttributeDefinitionCreateRequest request = getGlobalAttributeDefinitionCreateRequest(globalAttributeDefinitionKey);

        //create a test global attribute definition entity
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);

        //mock calls to external methods
        when(alternateKeyHelper.validateStringParameter("global attribute definition level", GLOBAL_ATTRIBUTE_DEFINITON_LEVEL))
            .thenReturn(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL);
        when(alternateKeyHelper.validateStringParameter("global attribute definition name", GLOBAL_ATTRIBUTE_DEFINITON_NAME_1))
            .thenReturn(GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        when(globalAttributeDefinitionDao.getGlobalAttributeDefinitionByKey(globalAttributeDefinitionKey)).thenReturn(null);
        when(globalAttributeDefinitionDao.saveAndRefresh(any(GlobalAttributeDefinitionEntity.class))).thenReturn(globalAttributeDefinitionEntity);

        //call method under test
        GlobalAttributeDefinition response = globalAttributeDefinitionService.createGlobalAttributeDefinition(request);

        assertEquals(new GlobalAttributeDefinition(response.getId(), globalAttributeDefinitionKey), response);
    }

    private GlobalAttributeDefinitionCreateRequest getGlobalAttributeDefinitionCreateRequest(GlobalAttributeDefinitionKey globalAttributeDefinitionKey)
    {
        return new GlobalAttributeDefinitionCreateRequest(globalAttributeDefinitionKey);
    }

    @Test
    public void testCreateGlobalAttributeDefinitionMissingRequiredParams()
    {

        try
        {
            globalAttributeDefinitionService.createGlobalAttributeDefinition(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A global attribute definition create request must be specified.", e.getMessage());
        }

        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_INVALID_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        GlobalAttributeDefinitionCreateRequest request = getGlobalAttributeDefinitionCreateRequest(globalAttributeDefinitionKey);
        when(alternateKeyHelper.validateStringParameter("global attribute definition level", GLOBAL_ATTRIBUTE_DEFINITON_INVALID_LEVEL))
            .thenReturn(GLOBAL_ATTRIBUTE_DEFINITON_INVALID_LEVEL);
        when(alternateKeyHelper.validateStringParameter("global attribute definition name", GLOBAL_ATTRIBUTE_DEFINITON_NAME_1))
            .thenReturn(GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);

        try
        {
            globalAttributeDefinitionService.createGlobalAttributeDefinition(request);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Global attribute definition with level \"%s\" is not supported.", GLOBAL_ATTRIBUTE_DEFINITON_INVALID_LEVEL),
                e.getMessage());
        }
    }

    @Test
    public void testDeleteGlobalAttributeDefinition()
    {
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);

        //create a test global attribute definition entity
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);

        when(globalAttributeDefinitionDaoHelper.getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey)).thenReturn(globalAttributeDefinitionEntity);

        GlobalAttributeDefinition response = globalAttributeDefinitionService.deleteGlobalAttributeDefinition(globalAttributeDefinitionKey);

        assertEquals(new GlobalAttributeDefinition(response.getId(), globalAttributeDefinitionKey), response);
    }

    @Test
    public void testGetGlobalAttributeDefinition()
    {
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey1 =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_2);

        when(globalAttributeDefinitionDao.getAllGlobalAttributeDefinitionKeys())
            .thenReturn(Arrays.asList(globalAttributeDefinitionKey, globalAttributeDefinitionKey1));
        GlobalAttributeDefinitionKeys response = globalAttributeDefinitionService.getGlobalAttributeDefinitionKeys();

        assertNotNull(response);
        assertEquals(response.getGlobalAttributeDefinitionKeies(), Arrays.asList(globalAttributeDefinitionKey, globalAttributeDefinitionKey1));
    }

    @Test
    public void testGetGlobalAttributeDefinitionEmpty()
    {
        when(globalAttributeDefinitionDao.getAllGlobalAttributeDefinitionKeys()).thenReturn(null);
        GlobalAttributeDefinitionKeys response = globalAttributeDefinitionService.getGlobalAttributeDefinitionKeys();
        assertTrue(response.getGlobalAttributeDefinitionKeies().size() == 0);
    }

}
