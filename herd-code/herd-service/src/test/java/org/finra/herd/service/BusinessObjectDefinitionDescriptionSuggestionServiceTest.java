package org.finra.herd.service;

import static junit.framework.TestCase.fail;
import static org.finra.herd.service.impl.BusinessObjectDefinitionDescriptionSuggestionServiceImpl.CREATED_BY_USER_ID_FIELD;
import static org.finra.herd.service.impl.BusinessObjectDefinitionDescriptionSuggestionServiceImpl.DESCRIPTION_SUGGESTION_FIELD;
import static org.finra.herd.service.impl.BusinessObjectDefinitionDescriptionSuggestionServiceImpl.STATUS_FIELD;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionCreateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchFilter;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchKey;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionSearchResponse;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionUpdateRequest;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionKey;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionDescriptionSuggestionStatusEntity;
import org.finra.herd.model.jpa.BusinessObjectDefinitionEntity;
import org.finra.herd.model.jpa.NamespaceEntity;
import org.finra.herd.service.helper.AlternateKeyHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDescriptionSuggestionDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionDescriptionSuggestionStatusDaoHelper;
import org.finra.herd.service.helper.BusinessObjectDefinitionHelper;
import org.finra.herd.service.impl.BusinessObjectDefinitionDescriptionSuggestionServiceImpl;

/**
 * Test for the business object definition description suggestion service implementation.
 */
public class BusinessObjectDefinitionDescriptionSuggestionServiceTest extends AbstractServiceTest
{
    @Mock
    private AlternateKeyHelper alternateKeyHelper;

    @Mock
    private BusinessObjectDefinitionDao businessObjectDefinitionDao;

    @Mock
    private BusinessObjectDefinitionDaoHelper businessObjectDefinitionDaoHelper;

    @Mock
    private BusinessObjectDefinitionDescriptionSuggestionDao businessObjectDefinitionDescriptionSuggestionDao;

    @Mock
    private BusinessObjectDefinitionDescriptionSuggestionDaoHelper businessObjectDefinitionDescriptionSuggestionDaoHelper;

    @Mock
    private BusinessObjectDefinitionDescriptionSuggestionStatusDaoHelper businessObjectDefinitionDescriptionSuggestionStatusDaoHelper;

    @Mock
    private BusinessObjectDefinitionHelper businessObjectDefinitionHelper;

    @InjectMocks
    private BusinessObjectDefinitionDescriptionSuggestionServiceImpl businessObjectDefinitionDescriptionSuggestionService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCreateBusinessObjectDefinitionDescriptionSuggestion()
    {
        // Create objects needed for test
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);
        BusinessObjectDefinitionDescriptionSuggestionCreateRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionCreateRequest(businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            new BusinessObjectDefinitionDescriptionSuggestionStatusEntity();
        businessObjectDefinitionDescriptionSuggestionStatusEntity
            .setCode(BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name());

        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setId(ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);

        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getId(),
                businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                businessObjectDefinitionDescriptionSuggestionEntity.getStatus().getCode(), businessObjectDefinitionDescriptionSuggestionEntity.getCreatedBy());

        // Mock the call to external methods
        when(alternateKeyHelper.validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        when(alternateKeyHelper
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName()))
            .thenReturn(BDEF_NAME);
        when(alternateKeyHelper.validateStringParameter("user id", businessObjectDefinitionDescriptionSuggestionKey.getUserId()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        when(businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionStatusDaoHelper.getBusinessObjectDefinitionDescriptionSuggestionStatusEntity(
            BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionStatusEntity);
        when(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionAndUserId(businessObjectDefinitionEntity,
                businessObjectDefinitionDescriptionSuggestionKey.getUserId())).thenReturn(null);
        when(businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(any(BusinessObjectDefinitionDescriptionSuggestionEntity.class)))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionEntity);

        // Call the method under test
        BusinessObjectDefinitionDescriptionSuggestion result =
            businessObjectDefinitionDescriptionSuggestionService.createBusinessObjectDefinitionDescriptionSuggestion(request);

        // Validate result
        assertThat("Result does not equal businessObjectDefinitionDescriptionSuggestionEntity.", result,
            is(equalTo(businessObjectDefinitionDescriptionSuggestion)));

        // Verify the calls to external methods
        verify(alternateKeyHelper).validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        verify(alternateKeyHelper)
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());
        verify(alternateKeyHelper).validateStringParameter("user id", businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verify(businessObjectDefinitionDaoHelper).getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionStatusDaoHelper).getBusinessObjectDefinitionDescriptionSuggestionStatusEntity(
            BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name());
        verify(businessObjectDefinitionDescriptionSuggestionDao)
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionAndUserId(businessObjectDefinitionEntity,
                businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verify(businessObjectDefinitionDescriptionSuggestionDao).saveAndRefresh(any(BusinessObjectDefinitionDescriptionSuggestionEntity.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDefinitionDescriptionSuggestionWithBusinessObjectDefinitionDescriptionSuggestionAlreadyExists()
    {
        // Create objects needed for test
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);
        BusinessObjectDefinitionDescriptionSuggestionCreateRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionCreateRequest(businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());

        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setId(ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);

        // Mock the call to external methods
        when(alternateKeyHelper.validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        when(alternateKeyHelper
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName()))
            .thenReturn(BDEF_NAME);
        when(alternateKeyHelper.validateStringParameter("user id", businessObjectDefinitionDescriptionSuggestionKey.getUserId()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        when(businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionAndUserId(businessObjectDefinitionEntity,
                businessObjectDefinitionDescriptionSuggestionKey.getUserId())).thenReturn(businessObjectDefinitionDescriptionSuggestionEntity);

        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService.createBusinessObjectDefinitionDescriptionSuggestion(request);
            fail();
        }
        catch (AlreadyExistsException alreadyExistsException)
        {
            // Validate result
            assertThat("Exception message is not correct.", alreadyExistsException.getMessage(), is(equalTo(String.format(
                "A business object definition description suggestion already exists with the parameters " +
                    "{namespace=\"%s\", businessObjectDefinitionName=\"%s\", userId=\"%s\"}.", businessObjectDefinitionDescriptionSuggestionKey.getNamespace(),
                businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName(),
                businessObjectDefinitionDescriptionSuggestionKey.getUserId()))));
        }

        // Verify the calls to external methods
        verify(businessObjectDefinitionDaoHelper).getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionDao)
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionAndUserId(businessObjectDefinitionEntity,
                businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verify(alternateKeyHelper).validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        verify(alternateKeyHelper)
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());
        verify(alternateKeyHelper).validateStringParameter("user id", businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDefinitionDescriptionSuggestionWithNullRequest()
    {
        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService.createBusinessObjectDefinitionDescriptionSuggestion(null);
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("A business object definition description suggestion create request must be specified.")));
        }

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDefinitionDescriptionSuggestionWithNullKey()
    {
        BusinessObjectDefinitionDescriptionSuggestionCreateRequest request = new BusinessObjectDefinitionDescriptionSuggestionCreateRequest(null, null);

        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService.createBusinessObjectDefinitionDescriptionSuggestion(request);
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("A business object definition description suggestion key must be specified.")));
        }

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testCreateBusinessObjectDefinitionDescriptionSuggestionWithNullDescriptionSuggestion()
    {
        // Create objects needed for test
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);
        BusinessObjectDefinitionDescriptionSuggestionCreateRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionCreateRequest(businessObjectDefinitionDescriptionSuggestionKey, null);

        // Mock the call to external methods
        when(alternateKeyHelper.validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        when(alternateKeyHelper
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName()))
            .thenReturn(BDEF_NAME);
        when(alternateKeyHelper.validateStringParameter("user id", businessObjectDefinitionDescriptionSuggestionKey.getUserId()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKey.getUserId());

        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService.createBusinessObjectDefinitionDescriptionSuggestion(request);
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("A business object definition description suggestion must be specified.")));
        }

        // Verify the calls to external methods
        verify(alternateKeyHelper).validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        verify(alternateKeyHelper)
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());
        verify(alternateKeyHelper).validateStringParameter("user id", businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteBusinessObjectDefinitionDescriptionSuggestion()
    {
        // Create objects needed for test
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            new BusinessObjectDefinitionDescriptionSuggestionStatusEntity();
        businessObjectDefinitionDescriptionSuggestionStatusEntity
            .setCode(BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name());

        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setId(ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);

        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getId(),
                businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                businessObjectDefinitionDescriptionSuggestionEntity.getStatus().getCode(), businessObjectDefinitionDescriptionSuggestionEntity.getCreatedBy());

        // Mock the call to external methods
        when(alternateKeyHelper.validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        when(alternateKeyHelper
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName()))
            .thenReturn(BDEF_NAME);
        when(alternateKeyHelper.validateStringParameter("user id", businessObjectDefinitionDescriptionSuggestionKey.getUserId()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        when(businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionDaoHelper.getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity,
            businessObjectDefinitionDescriptionSuggestionKey.getUserId())).thenReturn(businessObjectDefinitionDescriptionSuggestionEntity);

        // Call the method under test
        BusinessObjectDefinitionDescriptionSuggestion result = businessObjectDefinitionDescriptionSuggestionService
            .deleteBusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionKey);

        // Validate result
        assertThat("Result does not equal businessObjectDefinitionDescriptionSuggestionEntity.", result,
            is(equalTo(businessObjectDefinitionDescriptionSuggestion)));

        // Verify the calls to external methods
        verify(alternateKeyHelper).validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        verify(alternateKeyHelper)
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());
        verify(alternateKeyHelper).validateStringParameter("user id", businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verify(businessObjectDefinitionDaoHelper).getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionDaoHelper).getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity,
            businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verify(businessObjectDefinitionDescriptionSuggestionDao).delete(businessObjectDefinitionDescriptionSuggestionEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testDeleteBusinessObjectDefinitionDescriptionSuggestionWithNullKey()
    {
        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService.deleteBusinessObjectDefinitionDescriptionSuggestion(null);
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("A business object definition description suggestion key must be specified.")));
        }

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionByKey()
    {
        // Create objects needed for test
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            new BusinessObjectDefinitionDescriptionSuggestionStatusEntity();
        businessObjectDefinitionDescriptionSuggestionStatusEntity
            .setCode(BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name());

        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setId(ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);

        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getId(),
                businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                businessObjectDefinitionDescriptionSuggestionEntity.getStatus().getCode(), businessObjectDefinitionDescriptionSuggestionEntity.getCreatedBy());

        // Mock the call to external methods
        when(alternateKeyHelper.validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        when(alternateKeyHelper
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName()))
            .thenReturn(BDEF_NAME);
        when(alternateKeyHelper.validateStringParameter("user id", businessObjectDefinitionDescriptionSuggestionKey.getUserId()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        when(businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionDaoHelper.getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity,
            businessObjectDefinitionDescriptionSuggestionKey.getUserId())).thenReturn(businessObjectDefinitionDescriptionSuggestionEntity);

        // Call the method under test
        BusinessObjectDefinitionDescriptionSuggestion result = businessObjectDefinitionDescriptionSuggestionService
            .getBusinessObjectDefinitionDescriptionSuggestionByKey(businessObjectDefinitionDescriptionSuggestionKey);

        // Validate result
        assertThat("Result does not equal businessObjectDefinitionDescriptionSuggestionEntity.", result,
            is(equalTo(businessObjectDefinitionDescriptionSuggestion)));

        // Verify the calls to external methods
        verify(alternateKeyHelper).validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        verify(alternateKeyHelper)
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());
        verify(alternateKeyHelper).validateStringParameter("user id", businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verify(businessObjectDefinitionDaoHelper).getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionDaoHelper).getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity,
            businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestionWithNullKey()
    {
        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService.getBusinessObjectDefinitionDescriptionSuggestionByKey(null);
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("A business object definition description suggestion key must be specified.")));
        }

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testGetBusinessObjectDefinitionDescriptionSuggestions()
    {
        // Create objects needed for test
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey2 =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE_2, BDEF_NAME_2, USER_ID_2);

        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        List<BusinessObjectDefinitionDescriptionSuggestionKey> businessObjectDefinitionDescriptionSuggestionKeyList =
            Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionKey, businessObjectDefinitionDescriptionSuggestionKey2);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());

        // Mock the call to external methods
        when(businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinition(businessObjectDefinitionEntity))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKeyList);

        // Call the method under test
        List<BusinessObjectDefinitionDescriptionSuggestionKey> results =
            businessObjectDefinitionDescriptionSuggestionService.getBusinessObjectDefinitionDescriptionSuggestions(businessObjectDefinitionKey)
                .getBusinessObjectDefinitionDescriptionSuggestionKeys();

        // Validate results.
        for (int i = 0; i < results.size(); i++)
        {
            assertEquals(businessObjectDefinitionDescriptionSuggestionKeyList.get(i).getNamespace(), results.get(i).getNamespace());
            assertEquals(businessObjectDefinitionDescriptionSuggestionKeyList.get(i).getBusinessObjectDefinitionName(),
                results.get(i).getBusinessObjectDefinitionName());
            assertEquals(businessObjectDefinitionDescriptionSuggestionKeyList.get(i).getUserId(), results.get(i).getUserId());
        }

        // Verify the calls to external methods
        verify(businessObjectDefinitionHelper).validateBusinessObjectDefinitionKey(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDaoHelper).getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionDao)
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinition(businessObjectDefinitionEntity);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testSearchBusinessObjectDefinitionDescriptionSuggestions()
    {
        // Create objects needed for test
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            new BusinessObjectDefinitionDescriptionSuggestionStatusEntity();
        businessObjectDefinitionDescriptionSuggestionStatusEntity.setCode(BDEF_DESCRIPTION_SUGGESTION_STATUS);

        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setId(ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);

        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getId(),
                businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                businessObjectDefinitionDescriptionSuggestionEntity.getStatus().getCode(), businessObjectDefinitionDescriptionSuggestionEntity.getCreatedBy());

        // Create the business object definition description suggestion search request.
        BusinessObjectDefinitionDescriptionSuggestionSearchKey businessObjectDefinitionDescriptionSuggestionSearchKey =
            new BusinessObjectDefinitionDescriptionSuggestionSearchKey(NAMESPACE, BDEF_NAME, BDEF_DESCRIPTION_SUGGESTION_STATUS);
        BusinessObjectDefinitionDescriptionSuggestionSearchFilter businessObjectDefinitionDescriptionSuggestionSearchFilter =
            new BusinessObjectDefinitionDescriptionSuggestionSearchFilter(Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionSearchKey));
        BusinessObjectDefinitionDescriptionSuggestionSearchRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionSearchRequest(Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionSearchFilter));

        // Create the business object definition description suggestion search response.
        BusinessObjectDefinitionDescriptionSuggestionSearchResponse businessObjectDefinitionDescriptionSuggestionSearchResponse =
            new BusinessObjectDefinitionDescriptionSuggestionSearchResponse(Lists.newArrayList(businessObjectDefinitionDescriptionSuggestion));

        // Build the fields set
        Set<String> fields = new HashSet<>();
        fields.add(CREATED_BY_USER_ID_FIELD);
        fields.add(DESCRIPTION_SUGGESTION_FIELD);
        fields.add(STATUS_FIELD);

        List<BusinessObjectDefinitionDescriptionSuggestionEntity> businessObjectDefinitionDescriptionSuggestionEntities =
            Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionEntity);

        // Mock the call to external methods
        when(alternateKeyHelper.validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace())).thenReturn(NAMESPACE);
        when(alternateKeyHelper
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName()))
            .thenReturn(BDEF_NAME);
        when(businessObjectDefinitionDescriptionSuggestionDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionKey,
                BDEF_DESCRIPTION_SUGGESTION_STATUS)).thenReturn(businessObjectDefinitionDescriptionSuggestionEntities);

        // Call the method under test
        BusinessObjectDefinitionDescriptionSuggestionSearchResponse result =
            businessObjectDefinitionDescriptionSuggestionService.searchBusinessObjectDefinitionDescriptionSuggestions(request, fields);

        // Validate result
        assertThat("Result does not equal businessObjectDefinitionDescriptionSuggestionSearchResponse.", result,
            is(equalTo(businessObjectDefinitionDescriptionSuggestionSearchResponse)));

        // Verify the calls to external methods
        verify(alternateKeyHelper).validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        verify(alternateKeyHelper)
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());
        verify(businessObjectDefinitionDescriptionSuggestionDaoHelper)
            .getBusinessObjectDefinitionDescriptionSuggestionsByBusinessObjectDefinitionAndStatus(businessObjectDefinitionKey,
                BDEF_DESCRIPTION_SUGGESTION_STATUS);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testSearchBusinessObjectDefinitionDescriptionSuggestionsWithNullSearchRequest()
    {
        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService.searchBusinessObjectDefinitionDescriptionSuggestions(null, new HashSet<>());
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("A business object definition description suggestion search request must be specified.")));
        }

        // Verify the calls to external methods
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testSearchBusinessObjectDefinitionDescriptionSuggestionsWithWrongNumberOfSearchFilters()
    {
        // Zero search filters
        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService
                .searchBusinessObjectDefinitionDescriptionSuggestions(new BusinessObjectDefinitionDescriptionSuggestionSearchRequest(Lists.newArrayList()),
                    new HashSet<>());
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("Exactly one business object definition description suggestion search filter must be specified.")));
        }

        // Two search filters
        BusinessObjectDefinitionDescriptionSuggestionSearchFilter businessObjectDefinitionDescriptionSuggestionSearchFilter1 =
            new BusinessObjectDefinitionDescriptionSuggestionSearchFilter(Lists.newArrayList());
        BusinessObjectDefinitionDescriptionSuggestionSearchFilter businessObjectDefinitionDescriptionSuggestionSearchFilter2 =
            new BusinessObjectDefinitionDescriptionSuggestionSearchFilter(Lists.newArrayList());
        BusinessObjectDefinitionDescriptionSuggestionSearchRequest request = new BusinessObjectDefinitionDescriptionSuggestionSearchRequest(
            Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionSearchFilter1, businessObjectDefinitionDescriptionSuggestionSearchFilter2));

        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService.searchBusinessObjectDefinitionDescriptionSuggestions(request, new HashSet<>());
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("Exactly one business object definition description suggestion search filter must be specified.")));
        }

        // Verify the calls to external methods
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testSearchBusinessObjectDefinitionDescriptionSuggestionsWithZeroSearchKeys()
    {
        // Zero search keys
        BusinessObjectDefinitionDescriptionSuggestionSearchFilter businessObjectDefinitionDescriptionSuggestionSearchFilter =
            new BusinessObjectDefinitionDescriptionSuggestionSearchFilter(Lists.newArrayList());
        BusinessObjectDefinitionDescriptionSuggestionSearchRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionSearchRequest(Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionSearchFilter));

        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService.searchBusinessObjectDefinitionDescriptionSuggestions(request, new HashSet<>());
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("Exactly one business object definition description suggestion search key must be specified.")));
        }

        // Verify the calls to external methods
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testSearchBusinessObjectDefinitionDescriptionSuggestionsWithTwoSearchKeys()
    {
        // Two search keys
        BusinessObjectDefinitionDescriptionSuggestionSearchKey businessObjectDefinitionDescriptionSuggestionSearchKey1 =
            new BusinessObjectDefinitionDescriptionSuggestionSearchKey(NAMESPACE, BDEF_NAME, BDEF_DESCRIPTION_SUGGESTION_STATUS);
        BusinessObjectDefinitionDescriptionSuggestionSearchKey businessObjectDefinitionDescriptionSuggestionSearchKey2 =
            new BusinessObjectDefinitionDescriptionSuggestionSearchKey(NAMESPACE_2, BDEF_NAME_2, BDEF_DESCRIPTION_SUGGESTION_STATUS_2);
        BusinessObjectDefinitionDescriptionSuggestionSearchFilter businessObjectDefinitionDescriptionSuggestionSearchFilter =
            new BusinessObjectDefinitionDescriptionSuggestionSearchFilter(
                Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionSearchKey1, businessObjectDefinitionDescriptionSuggestionSearchKey2));
        BusinessObjectDefinitionDescriptionSuggestionSearchRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionSearchRequest(Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionSearchFilter));

        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService.searchBusinessObjectDefinitionDescriptionSuggestions(request, new HashSet<>());
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("Exactly one business object definition description suggestion search key must be specified.")));
        }

        // Verify the calls to external methods
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptionSuggestion()
    {
        // Create objects needed for test
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);
        BusinessObjectDefinitionDescriptionSuggestionUpdateRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionUpdateRequest(DESCRIPTION_SUGGESTION);

        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            new BusinessObjectDefinitionDescriptionSuggestionStatusEntity();
        businessObjectDefinitionDescriptionSuggestionStatusEntity
            .setCode(BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name());

        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setId(ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);

        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getId(),
                businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                businessObjectDefinitionDescriptionSuggestionEntity.getStatus().getCode(), businessObjectDefinitionDescriptionSuggestionEntity.getCreatedBy());

        // Mock the call to external methods
        when(alternateKeyHelper.validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        when(alternateKeyHelper
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName()))
            .thenReturn(BDEF_NAME);
        when(alternateKeyHelper.validateStringParameter("user id", businessObjectDefinitionDescriptionSuggestionKey.getUserId()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        when(businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionDaoHelper.getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity,
            businessObjectDefinitionDescriptionSuggestionKey.getUserId())).thenReturn(businessObjectDefinitionDescriptionSuggestionEntity);
        when(businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(any(BusinessObjectDefinitionDescriptionSuggestionEntity.class)))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionEntity);

        // Call the method under test
        BusinessObjectDefinitionDescriptionSuggestion result = businessObjectDefinitionDescriptionSuggestionService
            .updateBusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionKey, request);

        // Validate result
        assertThat("Result does not equal businessObjectDefinitionDescriptionSuggestionEntity.", result,
            is(equalTo(businessObjectDefinitionDescriptionSuggestion)));

        // Verify the calls to external methods
        verify(alternateKeyHelper).validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        verify(alternateKeyHelper)
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());
        verify(alternateKeyHelper).validateStringParameter("user id", businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verify(businessObjectDefinitionDaoHelper).getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionDaoHelper).getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity,
            businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verify(businessObjectDefinitionDescriptionSuggestionDao).saveAndRefresh(any(BusinessObjectDefinitionDescriptionSuggestionEntity.class));
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptionSuggestionWithNullRequest()
    {
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService
                .updateBusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionKey, null);
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("A business object definition description suggestion update request must be specified.")));
        }

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptionSuggestionWithNullKey()
    {
        BusinessObjectDefinitionDescriptionSuggestionUpdateRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionUpdateRequest(DESCRIPTION_SUGGESTION);
        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService.updateBusinessObjectDefinitionDescriptionSuggestion(null, request);
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("A business object definition description suggestion key must be specified.")));
        }

        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testUpdateBusinessObjectDefinitionDescriptionSuggestionWithNullDescriptionSuggestion()
    {
        // Create objects needed for test
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);
        BusinessObjectDefinitionDescriptionSuggestionUpdateRequest request = new BusinessObjectDefinitionDescriptionSuggestionUpdateRequest(null);

        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService
                .updateBusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionKey, request);
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("A business object definition description suggestion must be specified.")));
        }

        // Verify the calls to external methods
        verifyNoMoreInteractionsHelper();
    }

    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionDaoHelper,
            businessObjectDefinitionDescriptionSuggestionDao, businessObjectDefinitionDescriptionSuggestionDaoHelper);
    }
}
