package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import org.finra.herd.dao.GlobalAttributeDefinitionDao;
import org.finra.herd.dao.GlobalAttributeDefinitionLevelDao;
import org.finra.herd.model.api.xml.AttributeValueList;
import org.finra.herd.model.api.xml.AttributeValueListKey;
import org.finra.herd.model.api.xml.GlobalAttributeDefinition;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionCreateRequest;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKey;
import org.finra.herd.model.api.xml.GlobalAttributeDefinitionKeys;
import org.finra.herd.model.jpa.AttributeValueListEntity;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionEntity;
import org.finra.herd.model.jpa.GlobalAttributeDefinitionLevelEntity;
import org.finra.herd.service.helper.AttributeValueListDaoHelper;
import org.finra.herd.service.helper.AttributeValueListHelper;
import org.finra.herd.service.helper.GlobalAttributeDefinitionDaoHelper;
import org.finra.herd.service.helper.GlobalAttributeDefinitionHelper;
import org.finra.herd.service.impl.GlobalAttributeDefinitionServiceImpl;

/**
 * This class tests the functionality of global attribute definition service.
 */
public class GlobalAttributeDefinitionServiceTest extends AbstractServiceTest
{
    @Mock
    private GlobalAttributeDefinitionDao globalAttributeDefinitionDao;

    @Mock
    private GlobalAttributeDefinitionDaoHelper globalAttributeDefinitionDaoHelper;

    @Mock
    private GlobalAttributeDefinitionHelper globalAttributeDefinitionHelper;

    @Mock
    private GlobalAttributeDefinitionLevelDao globalAttributeDefinitionLevelDao;

    @Spy
    private AttributeValueListHelper attributeValueListHelper;

    @Spy
    private AttributeValueListDaoHelper attributeValueListDaoHelper;
    
    @InjectMocks
    private GlobalAttributeDefinitionServiceImpl globalAttributeDefinitionService;

    private AttributeValueListKey attributeValueListKeyNull = null;

    private AttributeValueList attributeValueListNull = null;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateGlobalAttributeDefinition()
    {
        // Create a global attribute definition key.
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Create a global attribute definition create request.
        GlobalAttributeDefinitionCreateRequest request = new GlobalAttributeDefinitionCreateRequest(globalAttributeDefinitionKey, attributeValueListKeyNull);

        // Create a global attribute definition level entity.
        GlobalAttributeDefinitionLevelEntity globalAttributeDefinitionLevelEntity = new GlobalAttributeDefinitionLevelEntity();
        globalAttributeDefinitionLevelEntity.setGlobalAttributeDefinitionLevel(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL);

        // Create a global attribute definition entity.
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity = new GlobalAttributeDefinitionEntity();
        globalAttributeDefinitionEntity.setId(GLOBAL_ATTRIBUTE_DEFINITON_ID);
        globalAttributeDefinitionEntity.setGlobalAttributeDefinitionLevel(globalAttributeDefinitionLevelEntity);
        globalAttributeDefinitionEntity.setGlobalAttributeDefinitionName(GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Mock calls to external methods.
        when(globalAttributeDefinitionLevelDao.getGlobalAttributeDefinitionLevel(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL))
            .thenReturn(globalAttributeDefinitionLevelEntity);
        when(globalAttributeDefinitionDao.saveAndRefresh(any(GlobalAttributeDefinitionEntity.class))).thenReturn(globalAttributeDefinitionEntity);

        // Call the method under test.
        GlobalAttributeDefinition response = globalAttributeDefinitionService.createGlobalAttributeDefinition(request);

        // Verify the external calls.
        verify(globalAttributeDefinitionHelper).validateGlobalAttributeDefinitionKey(globalAttributeDefinitionKey);
        verify(globalAttributeDefinitionDaoHelper).validateGlobalAttributeDefinitionNoExists(globalAttributeDefinitionKey);
        verify(globalAttributeDefinitionLevelDao).getGlobalAttributeDefinitionLevel(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL);
        verify(globalAttributeDefinitionDao).saveAndRefresh(any(GlobalAttributeDefinitionEntity.class));
        verifyNoMoreInteractionsHelper();

        // Validate the response.
        assertEquals(new GlobalAttributeDefinition(GLOBAL_ATTRIBUTE_DEFINITON_ID, globalAttributeDefinitionKey, attributeValueListNull), response);
    }

    @Test
    public void testCreateGlobalAttributeDefinitionMissingRequiredParameters()
    {
        // Try to call the method under test.
        try
        {
            globalAttributeDefinitionService.createGlobalAttributeDefinition(null);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("A global attribute definition create request must be specified.", e.getMessage());
        }

        // Verify the external calls.
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateGlobalAttributeDefinitionInvalidGlobalAttributeDefinitionLevel()
    {
        // Create a global attribute definition key with an unsupported global attribute definition level.
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_INVALID_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Create a global attribute definition create request.
        GlobalAttributeDefinitionCreateRequest request = new GlobalAttributeDefinitionCreateRequest(globalAttributeDefinitionKey, attributeValueListKeyNull);

        // Try to call the method under test.
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

        // Verify the external calls.
        verify(globalAttributeDefinitionHelper).validateGlobalAttributeDefinitionKey(globalAttributeDefinitionKey);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteGlobalAttributeDefinition()
    {
        // Create a global attribute definition key.
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Create a global attribute definition entity.
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Mock calls to external methods.
        when(globalAttributeDefinitionDaoHelper.getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey)).thenReturn(globalAttributeDefinitionEntity);

        // Call the method under test.
        GlobalAttributeDefinition response = globalAttributeDefinitionService.deleteGlobalAttributeDefinition(globalAttributeDefinitionKey);

        // Verify the external calls.
        verify(globalAttributeDefinitionHelper).validateGlobalAttributeDefinitionKey(globalAttributeDefinitionKey);
        verify(globalAttributeDefinitionDaoHelper).getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey);
        verify(globalAttributeDefinitionDao).delete(globalAttributeDefinitionEntity);
        verifyNoMoreInteractionsHelper();

        // Validate.
        assertEquals(new GlobalAttributeDefinition(response.getId(), globalAttributeDefinitionKey, attributeValueListNull), response);
    }

    @Test
    public void testGetGlobalAttributeDefinitions()
    {
        // Create a list of global attribute definitions keys.
        List<GlobalAttributeDefinitionKey> globalAttributeDefinitionKeys = Arrays
            .asList(new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME),
                new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME_2));

        // Mock calls to external methods.
        when(globalAttributeDefinitionDao.getAllGlobalAttributeDefinitionKeys()).thenReturn(globalAttributeDefinitionKeys);

        // Call the method under test.
        GlobalAttributeDefinitionKeys response = globalAttributeDefinitionService.getGlobalAttributeDefinitionKeys();

        // Verify the external calls.
        verify(globalAttributeDefinitionDao).getAllGlobalAttributeDefinitionKeys();
        verifyNoMoreInteractionsHelper();

        // Validate the response.
        assertEquals(new GlobalAttributeDefinitionKeys(globalAttributeDefinitionKeys), response);
    }

    @Test
    public void testGetGlobalAttributeDefinition()
    {
        // Create a global attribute definition key.
        GlobalAttributeDefinitionKey globalAttributeDefinitionKey =
            new GlobalAttributeDefinitionKey(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);

        // Create a global attribute definition entity.
        GlobalAttributeDefinitionEntity globalAttributeDefinitionEntity =
            globalAttributeDefinitionDaoTestHelper.createGlobalAttributeDefinitionEntity(GLOBAL_ATTRIBUTE_DEFINITON_LEVEL, GLOBAL_ATTRIBUTE_DEFINITON_NAME);
        AttributeValueListEntity attributeValueListEntity = attributeValueListDaoTestHelper.createAttributeValueListEntity("namespace_1", "list_1");
        globalAttributeDefinitionEntity.setAttributeValueList(attributeValueListEntity);
        
        // Mock calls to external methods.
        when(globalAttributeDefinitionDaoHelper.getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey)).thenReturn(globalAttributeDefinitionEntity);

        // Call the method under test.
        GlobalAttributeDefinition response = globalAttributeDefinitionService.getGlobalAttributeDefinition(globalAttributeDefinitionKey);

        // Verify the external calls.
        verify(globalAttributeDefinitionHelper).validateGlobalAttributeDefinitionKey(globalAttributeDefinitionKey);
        verify(globalAttributeDefinitionDaoHelper).getGlobalAttributeDefinitionEntity(globalAttributeDefinitionKey);
        verifyNoMoreInteractionsHelper();

        // Validate.
        assertEquals(new GlobalAttributeDefinition(response.getId(), globalAttributeDefinitionKey,
            attributeValueListDaoHelper.createAttributeValueListFromEntity(attributeValueListEntity)), response);
    }

    /**
     * Checks if any of the mocks has any interaction.
     */
    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(globalAttributeDefinitionDao, globalAttributeDefinitionDaoHelper, globalAttributeDefinitionHelper,
            globalAttributeDefinitionLevelDao);
    }
}
