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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import org.finra.herd.core.helper.ConfigurationHelper;
import org.finra.herd.dao.AwsClientFactory;
import org.finra.herd.dao.SesDao;
import org.finra.herd.dao.SesOperations;
import org.finra.herd.dao.helper.HerdStringHelper;
import org.finra.herd.model.dto.AwsParamsDto;
import org.finra.herd.model.dto.ConfigurationValue;
import org.finra.herd.model.dto.EmailDto;

/**
 * The SES DAO Implementation
 */
@Repository
public class SesDaoImpl implements SesDao
{

    @Autowired
    private AwsClientFactory awsClientFactory;

    @Autowired
    private SesOperations sesOperations;

    @Autowired
    private HerdStringHelper herdStringHelper;

    @Autowired
    private ConfigurationHelper configurationHelper;

    public SesDaoImpl()
    {

    }

    @Override
    public void sendEmail(final AwsParamsDto awsParamsDto, final EmailDto emailDto)
    {
        // prepare and fetch a send email request from the provided information
        SendEmailRequest sendEmailRequest = prepareSendEmailRequest(emailDto);

        // send the prepared email
        sesOperations.sendEmail(sendEmailRequest, awsClientFactory.getSesClient(awsParamsDto));
    }

    /**
     * Prepares a {@link SendEmailRequest} with information harvested from a specified {@link EmailDto}
     *
     * @param emailDto the specified email information.
     *
     * @return the prepared send email request.
     */
    private SendEmailRequest prepareSendEmailRequest(final EmailDto emailDto)
    {
        // Collect all required information and prepare individual email components required for the desired send email request

        // Initialize a new send email request
        SendEmailRequest sendEmailRequest = new SendEmailRequest();

        // set 'from' address to the configured 'send-from' email address
        sendEmailRequest.setSource(configurationHelper.getProperty(ConfigurationValue.ACTIVITI_DEFAULT_MAIL_FROM));

        // get destination information and add to send email request
        Destination destination = prepareDestination(emailDto);
        sendEmailRequest.setDestination(destination);

        // get message information and add to send email request
        Message message = prepareMessage(emailDto);
        sendEmailRequest.setMessage(message);

        // get config set name and add to send email request
        ConfigurationSet configurationSet = new ConfigurationSet()
            .withName(configurationHelper.getProperty(ConfigurationValue.SES_CONFIGURATION_SET_NAME));

        sendEmailRequest.setConfigurationSetName(configurationSet.getName());

        return sendEmailRequest;
    }

    /**
     * Prepares the destination part of the email based on the information contained in a provided {@link EmailDto}
     *
     * @param emailDto the specified email information.
     *
     * @return the prepared destination information.
     */
    private Destination prepareDestination(EmailDto emailDto)
    {
        // Initialize a new destinations object
        Destination destination = new Destination();

        // set 'to' addresses
        final String commaDelimiter = ",";

        destination.setToAddresses(
            herdStringHelper.splitAndTrim(emailDto.getTo(), commaDelimiter)
        );

        // set 'cc' addresses if specified
        if (Objects.nonNull(emailDto.getCc()))
        {
            destination.setCcAddresses(
                herdStringHelper.splitAndTrim(emailDto.getCc(), commaDelimiter)
            );
        }

        // declare a list of bcc addresses to set in the destinations being collected
        List<String> bccAddresses = new ArrayList<>();

        // get the 'records-collector' address and add it to the bcc addresses list if specified
        String recordsCollector = configurationHelper.getProperty(ConfigurationValue.SES_RECORDS_COLLECTOR_ADDRESS);
        if (Objects.nonNull(recordsCollector))
        {
            bccAddresses.add(recordsCollector);
        }

        // get 'bcc' addresses specified in the request
        if (Objects.nonNull(emailDto.getBcc()))
        {
            bccAddresses.addAll(
                herdStringHelper.splitAndTrim(emailDto.getBcc(), commaDelimiter)
            );
        }

        // add the final list of collected bcc addresses to destination
        if (!CollectionUtils.isEmpty(bccAddresses))
        {
            destination.setBccAddresses(bccAddresses);
        }

        return destination;
    }


    /**
     * Prepares the message part of the email based on the information contained in a provided {@link EmailDto}
     *
     * @param emailDto the specified email information
     *
     * @return the prepared message
     */
    private Message prepareMessage(EmailDto emailDto)
    {
        // Using UTF-8 which is a more commonly used charset. This is also the default for SES client.
        final String charset = "UTF-8";

        // Initialize an empty email message.
        Message message = new Message();

        // Insert subject if specified
        if (Objects.nonNull(emailDto.getSubject()))
        {
            message.setSubject(
                new Content()
                    .withCharset(charset)
                    .withData(emailDto.getSubject())
            );
        }

        // Insert text body if specified (this is the SES fall-back if the recipients email client does not support html).
        Body emailBody = new Body();
        if (Objects.nonNull(emailDto.getText()))
        {
            emailBody.setText(
                new Content()
                    .withCharset(charset)
                    .withData(emailDto.getText())
            );
        }

        // Insert html body if specified.
        if (Objects.nonNull(emailDto.getHtml()))
        {
            emailBody.setText(
                new Content()
                    .withCharset(charset)
                    .withData(emailDto.getHtml())
            );

        }

        // set the email body prepared above to the wrapper email message object.
        message.setBody(emailBody);

        return message;

    }


}
