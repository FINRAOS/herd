package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.GlobalAttributeDefinitionDao;
import org.finra.herd.dao.GlobalAttributeDefinitionLevelDao;
import org.finra.herd.dao.GlobalAttributeDefinitionLevelDaoTestHelper;
import org.finra.herd.model.api.xml.GlobalAttributeDefinition;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionCreateRequest;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKeys;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionEntity;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionLevelEntity;
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
    private GlobalAttributeDefinitionLevelDao globalAttributeDefinitionLevelDao;

    @Mock
    private GlobalAttributeDefinitionHelper globalAttributeDefinitionHelper;

    @Mock
    private GlobalAttributeDefinitionDaoHelper globalAttributeDefinitionDaoHelper;

    @Mock
    private GlobalAttributeDefinitionLevelDaoTestHelper globalAttributeDefinitionLevelDaoTestHelper;

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

        GlobalAttributeDefinitionLevelEntity globalAttributeDefinitionLevelEntity =
            globalAttributeDefinitionLevelDaoTestHelper.createGlobalAttributeDefinitionLevelEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL);

        //mock calls to external methods
        when(globalAttributeDefinitionLevelDao.getGlobalAttributeDefinitionLevel(globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel()))
            .thenReturn(globalAttributeDefinitionLevelEntity);
        when(globalAttributeDefinitionDao.saveAndRefresh(any(GlobalAttributeDefinitionEntity.class))).thenReturn(globalAttributeDefinitionEntity);

        //call method under test
        GlobalAttributeDefinition response = globalAttributeDefinitionService.createGlobalAttributeDefinition(request);

        //verify the calls
        verify(globalAttributeDefinitionDao).saveAndRefresh(any(GlobalAttributeDefinitionEntity.class));
        //verify(globalAttributeDefinitionLevelDao).getGlobalAttributeDefinitionLevel(globalAttributeDefinitionKey.getGlobalAttributeDefinitionLevel());
        verifyNoMoreInteractions(globalAttributeDefinitionDao, globalAttributeDefinitionLevelDao);

        //validate
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

        //mock calls to external methods
        when(globalAttributeDefinitionDaoHelper.getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey)).thenReturn(globalAttributeDefinitionEntity);

        //call method under test
        GlobalAttributeDefinition response = globalAttributeDefinitionService.deleteGlobalAttributeDefinition(globalAttributeDefinitionKey);

        //verify
        verify(globalAttributeDefinitionDaoHelper).getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey);
        verifyNoMoreInteractions(globalAttributeDefinitionDaoHelper);

        //validate
        assertEquals(new GlobalAttributeDefinition(response.getId(), globalAttributeDefinitionKey), response);
    }

    @Test
    public void testGetGlobalAttributeDefinition()
    {
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_1);
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey1 =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_2);

        //mock calls to external method
        when(globalAttributeDefinitionDao.getAllGlobalAttributeDefinitionKeys())
            .thenReturn(Arrays.asList(globalAttributeDefinitionKey, globalAttributeDefinitionKey1));

        //call method under test
        GlobalAttributeDefinitionKeys response = globalAttributeDefinitionService.getGlobalAttributeDefinitionKeys();

        //verify
        verify(globalAttributeDefinitionDao).getAllGlobalAttributeDefinitionKeys();
        verifyNoMoreInteractions(globalAttributeDefinitionDao);

        //validate
        assertNotNull(response);
        assertEquals(response.getGlobalAttributeDefinitionKeys(), Arrays.asList(globalAttributeDefinitionKey, globalAttributeDefinitionKey1));
    }
}
