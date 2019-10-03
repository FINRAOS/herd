package org.finra.herd.service;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.api.xml.EmailSendRequest;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.service.impl.ActivitiSesServiceImpl;
import org.finra.herd.service.impl.SesServiceImpl;

/**
 * Test cases for {@link org.finra.herd.service.impl.ActivitiSesServiceImpl}
 */
public class ActivitiSesServiceTest extends AbstractServiceTest
{
    @Mock
    private AwsHelper awsHelper;

    @Mock
    private SesServiceImpl sesService;

    @Mock
    private ConfigurationHelper configurationHelper;

    @InjectMocks
    private ActivitiSesServiceImpl activitiSesService;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSendActivitiEmail()
    {
        // Create an EmailSendRequest.
        EmailSendRequest emailSendRequest = new EmailSendRequest();
        emailSendRequest.setSource(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_DEFAULT_MAIL_FROM));

        // Call the method under test.
        when(awsHelper.getAwsParamsDto()).thenReturn(new AwsParamsDto());
        activitiSesService.sendEmail(emailSendRequest);

        // Verify the external calls.
        verify(sesService).sendEmail(emailSendRequest);
        verifyNoMoreInteractions(sesService);
    }
}
