package org.finra.herd.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.SesDao;
import org.finra.herd.dao.helper.AwsHelper;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.EmailDto;
import org.finra.herd.service.impl.SesServiceImpl;

/**
 * Test cases for {@link org.finra.herd.service.SesService}
 */
public class SesServiceTest extends AbstractServiceTest
{
    @Mock
    private SesDao sesDao;

    @Mock
    private AwsHelper awsHelper;

    @Mock
    private ConfigurationHelper configurationHelper;

    @InjectMocks
    private SesServiceImpl sesService;

    @Captor
    private ArgumentCaptor<EmailDto> emailDtoArgumentCaptor;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSendEmail()
    {
        // Create an Email DTO.
        EmailDto emailDto = new EmailDto();
        emailDto.setSource("test@abc.com");

        // Call the method under test.
        when(awsHelper.getAwsParamsDto()).thenReturn(new AwsParamsDto());
        sesService.sendEmail(emailDto);

        // Verify the external calls.
        verify(sesDao).sendEmail(new AwsParamsDto(), emailDto);
        verify(configurationHelper, never()).getProperty(ConfigurationValue.ACTIVITI_DEFAULT_MAIL_FROM);
        verifyNoMoreInteractions(sesDao);
    }

    @Test
    public void testSendEmailWithDefaultSourceValue()
    {
        // Create an Email DTO.
        EmailDto emailDto = new EmailDto();

        when(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_DEFAULT_MAIL_FROM))
            .thenReturn((String) ConfigurationValue.ACTIVITI_DEFAULT_MAIL_FROM.getDefaultValue());
        when(awsHelper.getAwsParamsDto()).thenReturn(new AwsParamsDto());

        // Call the method under test.
        sesService.sendEmail(emailDto);

        // Verify the external calls and default source value
        verify(sesDao).sendEmail(any(), emailDtoArgumentCaptor.capture());
        verify(configurationHelper).getProperty(ConfigurationValue.ACTIVITI_DEFAULT_MAIL_FROM);
        assertEquals(ConfigurationValue.ACTIVITI_DEFAULT_MAIL_FROM.getDefaultValue(), emailDtoArgumentCaptor.getValue().getSource());
        verifyNoMoreInteractions(sesDao);
    }
}
