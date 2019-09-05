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
package org.finra.herd.service.activiti.task;


import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.finra.herd.model.dto.EmailDto;
import org.finra.herd.service.SesService;

/**
 * Activiti task to send an email. <p> </p> </p>
 * <p>
 * <pre>
 *   <extensionElements>
 *     <activiti:field name="to" expression=""/>
 *     <activiti:field name="cc" expression=""/>
 *     <activiti:field name="bcc" expression=""/>
 *     <activiti:field name="subject" expression=""/>
 *     <activiti:field name="text" expression=""/>
 *     <activiti:field name="html" expression=""/>
 *     <activiti:field name="replyTo" expression=""/>
 *   </extensionElements>
 * </pre>
 */
@Component
public class SendEmail extends BaseJavaDelegate
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SendEmail.class);

    private Expression bcc;

    private Expression cc;

    private Expression html;

    private Expression replyTo;

    @Autowired
    private SesService sesService;

    private Expression source;

    private Expression subject;

    private Expression text;

    private Expression to;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        // populate email dto with information from the incoming send email request
        EmailDto emailDto = populateEmailDto(execution);

        // delegate to the corresponding service to send the email
        sesService.sendEmail(emailDto);
    }

    private EmailDto populateEmailDto(final DelegateExecution execution)
    {
        // Extract email information from incoming execution request and return a DTO
        EmailDto emailDto = EmailDto.builder().withSource(activitiHelper.getExpressionVariableAsString(source, execution))
            .withTo(activitiHelper.getExpressionVariableAsString(to, execution)).withCc(activitiHelper.getExpressionVariableAsString(cc, execution))
            .withBcc(activitiHelper.getExpressionVariableAsString(bcc, execution))
            .withSubject(activitiHelper.getRequiredExpressionVariableAsString(subject, execution, "subject"))
            .withText(activitiHelper.getExpressionVariableAsString(text, execution)).withHtml(activitiHelper.getExpressionVariableAsString(html, execution))
            .withReplyTo(activitiHelper.getExpressionVariableAsString(replyTo, execution)).build();

        LOGGER.info("Preparing to send email to recipient(s): \"{}\"", emailDto.getTo());

        return emailDto;
    }
}
