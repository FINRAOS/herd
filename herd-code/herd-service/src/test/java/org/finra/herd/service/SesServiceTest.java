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

        // Call the method under test.
        when(awsHelper.getAwsParamsDto()).thenReturn(new AwsParamsDto());
        sesService.sendEmail(emailDto);

        // Verify the external calls.
        verify(sesDao).sendEmail(new AwsParamsDto(), emailDto);
    }
}
