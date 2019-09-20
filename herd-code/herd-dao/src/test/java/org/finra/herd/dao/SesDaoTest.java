package org.finra.herd.dao;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.dao.impl.SesDaoImpl;
import org.finra.herd.model.api.xml.EmailSendRequest;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;


public class SesDaoTest extends AbstractDaoTest
{
    @Mock
    private SesOperations sesOperations;

    @Mock
    private AwsClientFactory awsClientFactory;

    @Mock
    private ConfigurationHelper configurationHelper;

    @Mock
    private HerdStringHelper herdStringHelper;

    @InjectMocks
    private SesDaoImpl sesDaoImpl;

    private static final String SES_SOURCE_ADDRESS = "from@abc.com";

    private static final String SES_TO_ADDRESS = "test1@abc.com,test2@abc.com";

    private static final String SES_CC_ADDRESS = "test3@abc.com";

    private static final String SES_BCC_ADDRESS = "";

    private static final String SES_SUBJECT = "test Email subject";

    private static final String SES_TXT = "sample test body";

    private static final String SES_HTML = null;

    private static final String SES_REPLYTO = null;

    private static final String COMMA_DELIMITER = ",";

    @Captor
    private ArgumentCaptor<SendEmailRequest> sendEmailRequestArgumentCaptor;

    @Before
    public void before()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSendEmail()
    {
        EmailSendRequest emailSendRequest = getDefaultEmailSendRequest();
        when(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_DEFAULT_MAIL_FROM)).thenReturn(SES_SOURCE_ADDRESS);
        when(configurationHelper.getProperty(ConfigurationValue.SES_RECORDS_COLLECTOR_ADDRESS))
            .thenReturn((String) ConfigurationValue.SES_RECORDS_COLLECTOR_ADDRESS.getDefaultValue());
        when(herdStringHelper.splitAndTrim(emailSendRequest.getTo(), COMMA_DELIMITER))
            .thenReturn(new HashSet<>(Arrays.asList(SES_TO_ADDRESS.split(COMMA_DELIMITER))));
        when(herdStringHelper.splitAndTrim(emailSendRequest.getCc(), COMMA_DELIMITER))
            .thenReturn(new HashSet<>(Arrays.asList(SES_CC_ADDRESS.split(COMMA_DELIMITER))));
        when(herdStringHelper.splitAndTrim(emailSendRequest.getBcc(), COMMA_DELIMITER))
            .thenReturn(new HashSet<>(Arrays.asList(SES_BCC_ADDRESS.split(COMMA_DELIMITER))));

        //Send email
        sesDaoImpl.sendEmail(getAwsParamsDto(), emailSendRequest);

        //Verify argument
        verify(sesOperations).sendEmail(sendEmailRequestArgumentCaptor.capture(), any());
        assertEquals(SES_SOURCE_ADDRESS, sendEmailRequestArgumentCaptor.getValue().getSource());
        assertEqualsIgnoreOrder("to Address not correct", Arrays.asList(SES_TO_ADDRESS.split(COMMA_DELIMITER)),
            sendEmailRequestArgumentCaptor.getValue().getDestination().getToAddresses());
        assertEquals(Collections.singletonList(SES_CC_ADDRESS), sendEmailRequestArgumentCaptor.getValue().getDestination().getCcAddresses());
        assertEquals(0, sendEmailRequestArgumentCaptor.getValue().getDestination().getBccAddresses().size());
        assertEquals(SES_SUBJECT, sendEmailRequestArgumentCaptor.getValue().getMessage().getSubject().getData());
        assertEquals(SES_TXT, sendEmailRequestArgumentCaptor.getValue().getMessage().getBody().getText().getData());
        assertNull(sendEmailRequestArgumentCaptor.getValue().getMessage().getBody().getHtml());
        assertNull(sendEmailRequestArgumentCaptor.getValue().getConfigurationSetName());
    }

    @Test
    public void testNullParameters()
    {
        EmailSendRequest emailSendRequest = getDefaultEmailSendRequest();
        when(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_DEFAULT_MAIL_FROM))
            .thenReturn((String) ConfigurationValue.ACTIVITI_DEFAULT_MAIL_FROM.getDefaultValue());
        when(configurationHelper.getProperty(ConfigurationValue.SES_RECORDS_COLLECTOR_ADDRESS))
            .thenReturn((String) ConfigurationValue.SES_RECORDS_COLLECTOR_ADDRESS.getDefaultValue());

        //Verify null value for parameters
        emailSendRequest.setTo(null);
        emailSendRequest.setBcc(null);
        emailSendRequest.setSubject(null);
        emailSendRequest.setText(null);
        emailSendRequest.setHtml(null);
        emailSendRequest.setReplyTo(null);
        sesDaoImpl.sendEmail(getAwsParamsDto(), emailSendRequest);
        verify(herdStringHelper, never()).splitAndTrim(null, COMMA_DELIMITER);
    }

    private AwsParamsDto getAwsParamsDto()
    {
        return new AwsParamsDto(NO_AWS_ACCESS_KEY, NO_AWS_SECRET_KEY, NO_SESSION_TOKEN, NO_HTTP_PROXY_HOST, NO_HTTP_PROXY_PORT, AWS_REGION_NAME_US_EAST_1);
    }

    private EmailSendRequest getDefaultEmailSendRequest()
    {
        return new EmailSendRequest(SES_TO_ADDRESS, SES_CC_ADDRESS, SES_BCC_ADDRESS, SES_SUBJECT, SES_TXT, SES_HTML, SES_REPLYTO);
    }
}
