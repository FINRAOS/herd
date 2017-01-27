package org.finra.herd.service.helper;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.listener.MessageListenerContainer;
import org.springframework.test.util.ReflectionTestUtils;

import org.finra.herd.core.ApplicationContextHolder;
import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.AbstractServiceTest;
import org.finra.herd.service.BusinessObjectDefinitionService;


public class SearchIndexUpdateJmsMessageListenerTest extends AbstractServiceTest
{
    @InjectMocks
    private SearchIndexUpdateJmsMessageListener searchIndexUpdateJmsMessageListener;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private BusinessObjectDefinitionService businessObjectDefinitionService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Configuration
    static class ContextConfiguration
    {
        @Bean(name = "org.springframework.jms.config.internalJmsListenerEndpointRegistry")
        JmsListenerEndpointRegistry registry()
        {
            //if Mockito not found return null
            try
            {
                Class.forName("org.mockito.Mockito");
            }
            catch (ClassNotFoundException ignored)
            {
                return null;
            }

            return Mockito.mock(JmsListenerEndpointRegistry.class);
        }
    }

    @Test
    public void testProcessMessage() throws Exception
    {
        // Call the method under test
        searchIndexUpdateJmsMessageListener.processMessage("PAYLOAD", null);

        // Verify the calls to external methods
        verify(businessObjectDefinitionService, times(1)).updateSearchIndexDocumentBusinessObjectDefinition("PAYLOAD");
    }

    @Test
    public void testControlListener()
    {
        ReflectionTestUtils.setField(searchIndexUpdateJmsMessageListener, "configurationHelper", configurationHelper);
        MessageListenerContainer mockMessageListenerContainer = Mockito.mock(MessageListenerContainer.class);

        // The listener is not enabled
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("false");
        JmsListenerEndpointRegistry registry = ApplicationContextHolder.getApplicationContext()
            .getBean("org.springframework.jms.config.internalJmsListenerEndpointRegistry", JmsListenerEndpointRegistry.class);
        when(registry.getListenerContainer(HerdJmsDestinationResolver.SQS_DESTINATION_SEARCH_INDEX_UPDATE_QUEUE)).thenReturn(mockMessageListenerContainer);

        // The listener is not running, nothing happened
        when(mockMessageListenerContainer.isRunning()).thenReturn(false);
        searchIndexUpdateJmsMessageListener.controlSearchIndexUpdateJmsMessageListener();
        verify(mockMessageListenerContainer, times(0)).stop();
        verify(mockMessageListenerContainer, times(0)).start();

        // The listener is running, but it is not enable, should stop
        when(mockMessageListenerContainer.isRunning()).thenReturn(true);
        searchIndexUpdateJmsMessageListener.controlSearchIndexUpdateJmsMessageListener();
        verify(mockMessageListenerContainer, times(1)).stop();

        // The listener is enabled
        when(configurationHelper.getProperty(ConfigurationValue.SEARCH_INDEX_UPDATE_JMS_LISTENER_ENABLED)).thenReturn("true");

        // The listener is running, should not call the start method
        when(mockMessageListenerContainer.isRunning()).thenReturn(true);
        searchIndexUpdateJmsMessageListener.controlSearchIndexUpdateJmsMessageListener();
        verify(mockMessageListenerContainer, times(0)).start();

        // The listener is not running, but it is enabled, should start
        when(mockMessageListenerContainer.isRunning()).thenReturn(false);
        searchIndexUpdateJmsMessageListener.controlSearchIndexUpdateJmsMessageListener();
        verify(mockMessageListenerContainer, times(1)).start();
    }
}
