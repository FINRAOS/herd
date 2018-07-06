package org.finra.herd.service;

import static junit.framework.TestCase.fail;
import static org.finra.herd.model.dto.SearchIndexUpdateDto.SEARCH_INDEX_UPDATE_TYPE_UPDATE;
import static org.finra.herd.service.impl.BusinessObjectDefinitionDescriptionSuggestionServiceImpl.CREATED_BY_USER_ID_FIELD;
import static org.finra.herd.service.impl.BusinessObjectDefinitionDescriptionSuggestionServiceImpl.CREATED_ON_FIELD;
import static org.finra.herd.service.impl.BusinessObjectDefinitionDescriptionSuggestionServiceImpl.DESCRIPTION_SUGGESTION_FIELD;
import static org.finra.herd.service.impl.BusinessObjectDefinitionDescriptionSuggestionServiceImpl.STATUS_FIELD;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.HerdDateUtils;
import org.finra.herd.dao.BusinessObjectDefinitionDao;
import org.finra.herd.dao.BusinessObjectDefinitionDescriptionSuggestionDao;
import org.finra.herd.model.AlreadyExistsException;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestion;
import org.finra.herd.model.api.xml.BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest;
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
import org.finra.herd.service.helper.SearchIndexUpdateHelper;
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

    @Mock
    private MessageNotificationEventService messageNotificationEventService;

    @Mock
    private SearchIndexUpdateHelper searchIndexUpdateHelper;

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
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        // Create a business object definition description suggestion create request.
        BusinessObjectDefinitionDescriptionSuggestionCreateRequest businessObjectDefinitionDescriptionSuggestionCreateRequest =
            new BusinessObjectDefinitionDescriptionSuggestionCreateRequest(businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION);

        // Create a namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(BDEF_NAME);

        // Create a business object definition description suggestion status entity.
        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            new BusinessObjectDefinitionDescriptionSuggestionStatusEntity();
        businessObjectDefinitionDescriptionSuggestionStatusEntity
            .setCode(BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name());

        // Create a business object definition description suggestion entity.
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setId(ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(USER_ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedBy(CREATED_BY);
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedOn(new Timestamp(CREATED_ON.toGregorianCalendar().getTimeInMillis()));
        businessObjectDefinitionDescriptionSuggestionEntity.setUpdatedBy(UPDATED_BY);
        businessObjectDefinitionDescriptionSuggestionEntity.setUpdatedOn(new Timestamp(UPDATED_ON.toGregorianCalendar().getTimeInMillis()));

        // Create an expected business object definition description suggestion response object.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name(), CREATED_BY,
                CREATED_ON);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("namespace", NAMESPACE)).thenReturn(NAMESPACE);
        when(alternateKeyHelper.validateStringParameter("business object definition name", BDEF_NAME)).thenReturn(BDEF_NAME);
        when(alternateKeyHelper.validateStringParameter("user id", USER_ID)).thenReturn(USER_ID);
        when(businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionStatusDaoHelper.getBusinessObjectDefinitionDescriptionSuggestionStatusEntity(
            BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name()))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionStatusEntity);
        when(businessObjectDefinitionDescriptionSuggestionDao
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionAndUserId(businessObjectDefinitionEntity,
                businessObjectDefinitionDescriptionSuggestionKey.getUserId())).thenReturn(null);
        when(businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(any(BusinessObjectDefinitionDescriptionSuggestionEntity.class)))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionEntity);

        // Call the method under test.
        BusinessObjectDefinitionDescriptionSuggestion result = businessObjectDefinitionDescriptionSuggestionService
            .createBusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionCreateRequest);

        // Validate the results.
        assertEquals(businessObjectDefinitionDescriptionSuggestion, result);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("namespace", NAMESPACE);
        verify(alternateKeyHelper).validateStringParameter("business object definition name", BDEF_NAME);
        verify(alternateKeyHelper).validateStringParameter("user id", USER_ID);
        verify(businessObjectDefinitionDaoHelper).getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionStatusDaoHelper).getBusinessObjectDefinitionDescriptionSuggestionStatusEntity(
            BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name());
        verify(businessObjectDefinitionDescriptionSuggestionDao)
            .getBusinessObjectDefinitionDescriptionSuggestionByBusinessObjectDefinitionAndUserId(businessObjectDefinitionEntity,
                businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verify(businessObjectDefinitionDescriptionSuggestionDao).saveAndRefresh(any(BusinessObjectDefinitionDescriptionSuggestionEntity.class));
        verify(messageNotificationEventService)
            .processBusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                namespaceEntity);
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
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedBy(CREATED_BY);
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedOn(new Timestamp(getRandomDate().getTime()));

        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getId(),
                businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                businessObjectDefinitionDescriptionSuggestionEntity.getStatus().getCode(), businessObjectDefinitionDescriptionSuggestionEntity.getCreatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionDescriptionSuggestionEntity.getCreatedOn()));

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
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedBy(CREATED_BY);
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedOn(new Timestamp(getRandomDate().getTime()));

        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getId(),
                businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                businessObjectDefinitionDescriptionSuggestionEntity.getStatus().getCode(), businessObjectDefinitionDescriptionSuggestionEntity.getCreatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionDescriptionSuggestionEntity.getCreatedOn()));

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
        // Build the fields set
        Set<String> fields = new HashSet<>();
        fields.add(CREATED_BY_USER_ID_FIELD);
        fields.add(CREATED_ON_FIELD);
        fields.add(DESCRIPTION_SUGGESTION_FIELD);
        fields.add(STATUS_FIELD);

        testSearchBusinessObjectDefinitionDescriptionSuggestionsWithDifferentFields(fields);

        fields = new HashSet<>();
        fields.add(CREATED_ON_FIELD);
        fields.add(DESCRIPTION_SUGGESTION_FIELD);
        fields.add(STATUS_FIELD);

        testSearchBusinessObjectDefinitionDescriptionSuggestionsWithDifferentFields(fields);

        fields = new HashSet<>();
        fields.add(DESCRIPTION_SUGGESTION_FIELD);
        fields.add(STATUS_FIELD);

        testSearchBusinessObjectDefinitionDescriptionSuggestionsWithDifferentFields(fields);

        fields = new HashSet<>();
        fields.add(STATUS_FIELD);

        testSearchBusinessObjectDefinitionDescriptionSuggestionsWithDifferentFields(fields);

        fields = new HashSet<>();
        testSearchBusinessObjectDefinitionDescriptionSuggestionsWithDifferentFields(fields);

        fields = new HashSet<>();
        fields.add(CREATED_BY_USER_ID_FIELD);

        testSearchBusinessObjectDefinitionDescriptionSuggestionsWithDifferentFields(fields);

        fields = new HashSet<>();
        fields.add(CREATED_BY_USER_ID_FIELD);
        fields.add(CREATED_ON_FIELD);

        testSearchBusinessObjectDefinitionDescriptionSuggestionsWithDifferentFields(fields);

        fields = new HashSet<>();
        fields.add(CREATED_BY_USER_ID_FIELD);
        fields.add(CREATED_ON_FIELD);
        fields.add(DESCRIPTION_SUGGESTION_FIELD);

        testSearchBusinessObjectDefinitionDescriptionSuggestionsWithDifferentFields(fields);
    }

    private void testSearchBusinessObjectDefinitionDescriptionSuggestionsWithDifferentFields(Set<String> fields)
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

        Date createdOn = getRandomDate();

        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setId(ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedBy(CREATED_BY);
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedOn(new Timestamp(createdOn.getTime()));

        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion = new BusinessObjectDefinitionDescriptionSuggestion();
        businessObjectDefinitionDescriptionSuggestion.setId(businessObjectDefinitionDescriptionSuggestionEntity.getId());
        businessObjectDefinitionDescriptionSuggestion.setBusinessObjectDefinitionDescriptionSuggestionKey(businessObjectDefinitionDescriptionSuggestionKey);

        if (fields.contains(CREATED_BY_USER_ID_FIELD))
        {
            businessObjectDefinitionDescriptionSuggestion.setCreatedByUserId(CREATED_BY);
        }

        if (fields.contains(CREATED_ON_FIELD))
        {
            businessObjectDefinitionDescriptionSuggestion.setCreatedOn(HerdDateUtils.getXMLGregorianCalendarValue(createdOn));
        }

        if (fields.contains(DESCRIPTION_SUGGESTION_FIELD))
        {
            businessObjectDefinitionDescriptionSuggestion
                .setDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getDescriptionSuggestion());
        }

        if (fields.contains(STATUS_FIELD))
        {
            businessObjectDefinitionDescriptionSuggestion.setStatus(businessObjectDefinitionDescriptionSuggestionEntity.getStatus().getCode());
        }

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

        // Reset the verify counts because this method is called multiple times by the testSearchBusinessObjectDefinitionDescriptionSuggestions test method.
        reset(alternateKeyHelper);
        reset(businessObjectDefinitionDescriptionSuggestionDaoHelper);
    }

    @Test
    public void testSearchBusinessObjectDefinitionDescriptionSuggestionsWithBogusField()
    {
        // Create objects needed for test
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);
        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            new BusinessObjectDefinitionDescriptionSuggestionStatusEntity();
        businessObjectDefinitionDescriptionSuggestionStatusEntity.setCode(BDEF_DESCRIPTION_SUGGESTION_STATUS);

        // Create the business object definition description suggestion search request.
        BusinessObjectDefinitionDescriptionSuggestionSearchKey businessObjectDefinitionDescriptionSuggestionSearchKey =
            new BusinessObjectDefinitionDescriptionSuggestionSearchKey(NAMESPACE, BDEF_NAME, BDEF_DESCRIPTION_SUGGESTION_STATUS);
        BusinessObjectDefinitionDescriptionSuggestionSearchFilter businessObjectDefinitionDescriptionSuggestionSearchFilter =
            new BusinessObjectDefinitionDescriptionSuggestionSearchFilter(Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionSearchKey));
        BusinessObjectDefinitionDescriptionSuggestionSearchRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionSearchRequest(Lists.newArrayList(businessObjectDefinitionDescriptionSuggestionSearchFilter));

        // Mock the call to external methods
        when(alternateKeyHelper.validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace())).thenReturn(NAMESPACE);
        when(alternateKeyHelper
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName()))
            .thenReturn(BDEF_NAME);

        // Build the fields set
        Set<String> fields = new HashSet<>();
        fields.add(BOGUS_SEARCH_FIELD);

        try
        {
            // Call the method under test
            businessObjectDefinitionDescriptionSuggestionService.searchBusinessObjectDefinitionDescriptionSuggestions(request, fields);
            fail();
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate result
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo(String.format("Search response field \"%s\" is not supported.", BOGUS_SEARCH_FIELD))));
        }

        // Verify the calls to external methods
        verify(alternateKeyHelper).validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        verify(alternateKeyHelper)
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());
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
        // Create a business object definition key.
        BusinessObjectDefinitionKey businessObjectDefinitionKey = new BusinessObjectDefinitionKey(NAMESPACE, BDEF_NAME);

        // Create a business object definition description suggestion key.
        BusinessObjectDefinitionDescriptionSuggestionKey businessObjectDefinitionDescriptionSuggestionKey =
            new BusinessObjectDefinitionDescriptionSuggestionKey(NAMESPACE, BDEF_NAME, USER_ID);

        // Create a business object definition description suggestion update request.
        BusinessObjectDefinitionDescriptionSuggestionUpdateRequest businessObjectDefinitionDescriptionSuggestionUpdateRequest =
            new BusinessObjectDefinitionDescriptionSuggestionUpdateRequest(DESCRIPTION_SUGGESTION_2);

        // Create a namespace entity.
        NamespaceEntity namespaceEntity = new NamespaceEntity();
        namespaceEntity.setCode(NAMESPACE);

        // Create a business object definition entity.
        BusinessObjectDefinitionEntity businessObjectDefinitionEntity = new BusinessObjectDefinitionEntity();
        businessObjectDefinitionEntity.setNamespace(namespaceEntity);
        businessObjectDefinitionEntity.setName(BDEF_NAME);

        // Create a business object definition description suggestion status entity.
        BusinessObjectDefinitionDescriptionSuggestionStatusEntity businessObjectDefinitionDescriptionSuggestionStatusEntity =
            new BusinessObjectDefinitionDescriptionSuggestionStatusEntity();
        businessObjectDefinitionDescriptionSuggestionStatusEntity
            .setCode(BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name());

        // Create a business object definition description suggestion entity.
        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setId(ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(USER_ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedBy(CREATED_BY);
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedOn(new Timestamp(CREATED_ON.toGregorianCalendar().getTimeInMillis()));
        businessObjectDefinitionDescriptionSuggestionEntity.setUpdatedBy(UPDATED_BY);
        businessObjectDefinitionDescriptionSuggestionEntity.setUpdatedOn(new Timestamp(UPDATED_ON.toGregorianCalendar().getTimeInMillis()));

        // Create an expected business object definition description suggestion response object.
        BusinessObjectDefinitionDescriptionSuggestion businessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(ID, businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION_2,
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.PENDING.name(), CREATED_BY,
                CREATED_ON);

        // Mock the external calls.
        when(alternateKeyHelper.validateStringParameter("namespace", NAMESPACE)).thenReturn(NAMESPACE);
        when(alternateKeyHelper.validateStringParameter("business object definition name", BDEF_NAME)).thenReturn(BDEF_NAME);
        when(alternateKeyHelper.validateStringParameter("user id", USER_ID)).thenReturn(USER_ID);
        when(businessObjectDefinitionDaoHelper.getBusinessObjectDefinitionEntity(businessObjectDefinitionKey)).thenReturn(businessObjectDefinitionEntity);
        when(businessObjectDefinitionDescriptionSuggestionDaoHelper
            .getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity, USER_ID))
            .thenReturn(businessObjectDefinitionDescriptionSuggestionEntity);

        // Call the method under test.
        BusinessObjectDefinitionDescriptionSuggestion result = businessObjectDefinitionDescriptionSuggestionService
            .updateBusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionKey,
                businessObjectDefinitionDescriptionSuggestionUpdateRequest);

        // Validate the results.
        assertEquals(businessObjectDefinitionDescriptionSuggestion, result);

        // Verify the external calls.
        verify(alternateKeyHelper).validateStringParameter("namespace", NAMESPACE);
        verify(alternateKeyHelper).validateStringParameter("business object definition name", BDEF_NAME);
        verify(alternateKeyHelper).validateStringParameter("user id", USER_ID);
        verify(businessObjectDefinitionDaoHelper).getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionDaoHelper).getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity,
            businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verify(businessObjectDefinitionDescriptionSuggestionDao).saveAndRefresh(businessObjectDefinitionDescriptionSuggestionEntity);
        verify(messageNotificationEventService)
            .processBusinessObjectDefinitionDescriptionSuggestionChangeNotificationEvent(businessObjectDefinitionDescriptionSuggestion, UPDATED_BY, UPDATED_ON,
                namespaceEntity);
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

    @Test
    public void testAcceptBusinessObjectDefinitionDescriptionSuggestion()
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
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedBy(CREATED_BY);
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedOn(new Timestamp(getRandomDate().getTime()));

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity acceptedBusinessObjectDefinitionDescriptionSuggestionStatusEntity =
            new BusinessObjectDefinitionDescriptionSuggestionStatusEntity();
        acceptedBusinessObjectDefinitionDescriptionSuggestionStatusEntity
            .setCode(BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.ACCEPTED.name());

        BusinessObjectDefinitionDescriptionSuggestionEntity acceptedBusinessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        acceptedBusinessObjectDefinitionDescriptionSuggestionEntity.setStatus(acceptedBusinessObjectDefinitionDescriptionSuggestionStatusEntity);

        BusinessObjectDefinitionDescriptionSuggestion acceptedBusinessObjectDefinitionDescriptionSuggestion =
            new BusinessObjectDefinitionDescriptionSuggestion(businessObjectDefinitionDescriptionSuggestionEntity.getId(),
                businessObjectDefinitionDescriptionSuggestionKey, DESCRIPTION_SUGGESTION,
                BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.ACCEPTED.name(),
                businessObjectDefinitionDescriptionSuggestionEntity.getCreatedBy(),
                HerdDateUtils.getXMLGregorianCalendarValue(businessObjectDefinitionDescriptionSuggestionEntity.getCreatedOn()));

        // mock the calls
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
        when(businessObjectDefinitionDescriptionSuggestionStatusDaoHelper.getBusinessObjectDefinitionDescriptionSuggestionStatusEntity(anyString()))
            .thenReturn(acceptedBusinessObjectDefinitionDescriptionSuggestionStatusEntity);
        when(businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(any(BusinessObjectDefinitionDescriptionSuggestionEntity.class)))
            .thenReturn(acceptedBusinessObjectDefinitionDescriptionSuggestionEntity);
        when(businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity)).thenReturn(businessObjectDefinitionEntity);

        // Create business object definition description suggestion acceptance request.
        BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest(businessObjectDefinitionDescriptionSuggestionKey);

        // Call the method under test.
        BusinessObjectDefinitionDescriptionSuggestion response =
            businessObjectDefinitionDescriptionSuggestionService.acceptBusinessObjectDefinitionDescriptionSuggestion(request);

        // Validate response
        assertThat("Response does not equal to expected businessObjectDefinitionDescriptionSuggestionEntity.", response,
            is(equalTo(acceptedBusinessObjectDefinitionDescriptionSuggestion)));
        assertThat("Expected business object definition entity description to be equal to response description.", response.getDescriptionSuggestion(),
            is(equalTo(businessObjectDefinitionEntity.getDescription())));

        // Verify the calls to external methods
        verify(alternateKeyHelper).validateStringParameter("namespace", businessObjectDefinitionDescriptionSuggestionKey.getNamespace());
        verify(alternateKeyHelper)
            .validateStringParameter("business object definition name", businessObjectDefinitionDescriptionSuggestionKey.getBusinessObjectDefinitionName());
        verify(alternateKeyHelper).validateStringParameter("user id", businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verify(businessObjectDefinitionDaoHelper).getBusinessObjectDefinitionEntity(businessObjectDefinitionKey);
        verify(businessObjectDefinitionDescriptionSuggestionStatusDaoHelper).getBusinessObjectDefinitionDescriptionSuggestionStatusEntity(
            BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.ACCEPTED.name());
        verify(businessObjectDefinitionDescriptionSuggestionDao).saveAndRefresh(businessObjectDefinitionDescriptionSuggestionEntity);
        verify(businessObjectDefinitionDescriptionSuggestionDaoHelper).getBusinessObjectDefinitionDescriptionSuggestionEntity(businessObjectDefinitionEntity,
            businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        verify(businessObjectDefinitionDaoHelper).saveBusinessObjectDefinitionChangeEvents(businessObjectDefinitionEntity);
        verify(businessObjectDefinitionDao).saveAndRefresh(businessObjectDefinitionEntity);
        verify(searchIndexUpdateHelper).modifyBusinessObjectDefinitionInSearchIndex(businessObjectDefinitionEntity, SEARCH_INDEX_UPDATE_TYPE_UPDATE);
        verifyNoMoreInteractionsHelper();
    }

    @Test
    public void testAcceptBusinessObjectDefinitionDescriptionSuggestionWithNullRequest()
    {
        // Call business object definition description suggestion acceptance with null request.
        try
        {
            businessObjectDefinitionDescriptionSuggestionService.acceptBusinessObjectDefinitionDescriptionSuggestion(null);
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate response
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("A business object definition description suggestion acceptance request must be specified.")));
        }
    }

    @Test
    public void testAcceptBusinessObjectDefinitionDescriptionSuggestionWithSuggestionStatusNotPendingStatus()
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
            .setCode(BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.ACCEPTED.name());

        BusinessObjectDefinitionDescriptionSuggestionEntity businessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        businessObjectDefinitionDescriptionSuggestionEntity.setId(ID);
        businessObjectDefinitionDescriptionSuggestionEntity.setBusinessObjectDefinition(businessObjectDefinitionEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setUserId(businessObjectDefinitionDescriptionSuggestionKey.getUserId());
        businessObjectDefinitionDescriptionSuggestionEntity.setDescriptionSuggestion(DESCRIPTION_SUGGESTION);
        businessObjectDefinitionDescriptionSuggestionEntity.setStatus(businessObjectDefinitionDescriptionSuggestionStatusEntity);
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedBy(CREATED_BY);
        businessObjectDefinitionDescriptionSuggestionEntity.setCreatedOn(new Timestamp(getRandomDate().getTime()));

        BusinessObjectDefinitionDescriptionSuggestionStatusEntity acceptedBusinessObjectDefinitionDescriptionSuggestionStatusEntity =
            new BusinessObjectDefinitionDescriptionSuggestionStatusEntity();
        acceptedBusinessObjectDefinitionDescriptionSuggestionStatusEntity
            .setCode(BusinessObjectDefinitionDescriptionSuggestionStatusEntity.BusinessObjectDefinitionDescriptionSuggestionStatuses.ACCEPTED.name());

        BusinessObjectDefinitionDescriptionSuggestionEntity acceptedBusinessObjectDefinitionDescriptionSuggestionEntity =
            new BusinessObjectDefinitionDescriptionSuggestionEntity();
        acceptedBusinessObjectDefinitionDescriptionSuggestionEntity.setStatus(acceptedBusinessObjectDefinitionDescriptionSuggestionStatusEntity);

        // mock the calls
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
        when(businessObjectDefinitionDescriptionSuggestionStatusDaoHelper.getBusinessObjectDefinitionDescriptionSuggestionStatusEntity(anyString()))
            .thenReturn(acceptedBusinessObjectDefinitionDescriptionSuggestionStatusEntity);
        when(businessObjectDefinitionDescriptionSuggestionDao.saveAndRefresh(any(BusinessObjectDefinitionDescriptionSuggestionEntity.class)))
            .thenReturn(acceptedBusinessObjectDefinitionDescriptionSuggestionEntity);
        when(businessObjectDefinitionDao.saveAndRefresh(businessObjectDefinitionEntity)).thenReturn(businessObjectDefinitionEntity);

        // Create business object definition description suggestion acceptance request.
        BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest request =
            new BusinessObjectDefinitionDescriptionSuggestionAcceptanceRequest(businessObjectDefinitionDescriptionSuggestionKey);

        // Call the method under test and Validate the response
        try
        {
            businessObjectDefinitionDescriptionSuggestionService.acceptBusinessObjectDefinitionDescriptionSuggestion(request);
        }
        catch (IllegalArgumentException illegalArgumentException)
        {
            // Validate response
            assertThat("Exception message is not correct.", illegalArgumentException.getMessage(),
                is(equalTo("A business object definition description suggestion status is expected to be \"PENDING\" but was \"ACCEPTED\".")));
        }
    }

    private void verifyNoMoreInteractionsHelper()
    {
        verifyNoMoreInteractions(alternateKeyHelper, businessObjectDefinitionDao, businessObjectDefinitionDaoHelper,
            businessObjectDefinitionDescriptionSuggestionDao, businessObjectDefinitionDescriptionSuggestionDaoHelper,
            businessObjectDefinitionDescriptionSuggestionStatusDaoHelper, businessObjectDefinitionHelper, messageNotificationEventService,
            searchIndexUpdateHelper);
    }
}
