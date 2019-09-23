/*
 * Copyright 2015 herd contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.finra.herd.dao.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.ConfigurationSet;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.Message;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.AwsClientFactory;
import org.finra.herd.dao.SesDao;
import org.finra.herd.dao.SesOperations;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.api.xml.EmailSendRequest;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;

/**
 * The SES DAO Implementation
 */
@Repository
public class SesDaoImpl implements SesDao
{
    @Autowired
    private AwsClientFactory awsClientFactory;

    @Autowired
    private ConfigurationHelper configurationHelper;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private SesOperations sesOperations;

    @Override
    public void sendEmail(final AwsParamsDto awsParamsDto, EmailSendRequest emailSendRequest)
    {
        // Prepare and fetch a send email request from the provided information
        SendEmailRequest awsSendEmailRequest = prepareSendEmailRequest(emailSendRequest);

        // Send the prepared email
        sesOperations.sendEmail(awsSendEmailRequest, awsClientFactory.getSesClient(awsParamsDto));
    }

    /**
     * Prepares the destination part of the email based on the information in the emailSendRequest
     *
     * @param emailSendRequest the specified email information.
     *
     * @return the prepared destination information.
     */
    private Destination prepareDestination(EmailSendRequest emailSendRequest)
    {
        // Initialize a new destinations object.
        Destination destination = new Destination();
        final String commaDelimiter = ",";

        // Set 'to' addresses.
        if (StringUtils.isNotEmpty(emailSendRequest.getTo()))
        {
            destination.setToAddresses(herdStringHelper.splitAndTrim(emailSendRequest.getTo(), commaDelimiter));
        }

        // Set 'cc' addresses if specified.
        if (StringUtils.isNotEmpty(emailSendRequest.getCc()))
        {
            destination.setCcAddresses(herdStringHelper.splitAndTrim(emailSendRequest.getCc(), commaDelimiter));
        }

        // Declare a list of bcc addresses to set in the destinations being collected.
        List<String> bccAddresses = new ArrayList<>();

        // Get the 'records-collector' address and add it to the bcc addresses list if specified.
        String recordsCollector = configurationHelper.getProperty(ConfigurationValue.SES_RECORDS_COLLECTOR_ADDRESS);
        if (StringUtils.isNotEmpty(recordsCollector))
        {
            bccAddresses.add(recordsCollector);
        }

        // Get 'bcc' addresses specified in the request.
        if (StringUtils.isNotEmpty(emailSendRequest.getBcc()))
        {
            bccAddresses.addAll(herdStringHelper.splitAndTrim(emailSendRequest.getBcc(), commaDelimiter));
        }

        // Add the final list of collected bcc addresses to destination.
        if (!CollectionUtils.isEmpty(bccAddresses))
        {
            destination.setBccAddresses(bccAddresses);
        }

        return destination;
    }

    /**
     * Prepares the message part of the email based on the information in the emailSendRequest
     *
     * @param emailSendRequest the specified email information
     *
     * @return the prepared message
     */
    private Message prepareMessage(EmailSendRequest emailSendRequest)
    {
        // Using UTF-8 which is a more commonly used charset. This is also the default for SES client.
        final String charset = "UTF-8";

        // Initialize an empty email message.
        Message message = new Message();

        // Insert subject if specified.
        if (Objects.nonNull(emailSendRequest.getSubject()))
        {
            message.setSubject(new Content().withCharset(charset).withData(emailSendRequest.getSubject()));
        }

        // Insert text body if specified (this is the SES fall-back if the recipients email client does not support html).
        Body emailBody = new Body();
        if (Objects.nonNull(emailSendRequest.getText()))
        {
            emailBody.setText(new Content().withCharset(charset).withData(emailSendRequest.getText()));
        }

        // Set the email body prepared above to the wrapper email message object.
        message.setBody(emailBody);

        return message;
    }

    /**
     * Prepares a {@link SendEmailRequest} with information harvested from emailSendRequest
     *
     * @param emailSendRequest the specified email information.
     *
     * @return the prepared send email request.
     */
    private SendEmailRequest prepareSendEmailRequest(EmailSendRequest emailSendRequest)
    {
        // Collect all required information and prepare individual email components required for the desired send email request.
        // Initialize a new aws send email request.
        SendEmailRequest sendEmailRequest = new SendEmailRequest();

        // Set 'from' address to the configured 'send-from' email address.
        sendEmailRequest.setSource(emailSendRequest.getSource());

        // Get destination information and add to send email request
        Destination destination = prepareDestination(emailSendRequest);
        sendEmailRequest.setDestination(destination);

        // Get message information and add to send email request
        Message message = prepareMessage(emailSendRequest);
        sendEmailRequest.setMessage(message);

        // Get config set name and add to send email request
        ConfigurationSet configurationSet = new ConfigurationSet().withName(configurationHelper.getProperty(ConfigurationValue.SES_CONFIGURATION_SET_NAME));

        sendEmailRequest.setConfigurationSetName(configurationSet.getName());

        return sendEmailRequest;
    }
}
