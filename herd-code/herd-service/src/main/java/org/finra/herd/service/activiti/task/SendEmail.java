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

import org.finra.herd.model.api.xml.EmailSendRequest;
import org.finra.herd.service.impl.ActivitiSesServiceImpl;

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

    private Expression replyTo;

    @Autowired
    private ActivitiSesServiceImpl activitiSesService;

    private Expression subject;

    private Expression text;

    private Expression to;

    @Override
    public void executeImpl(DelegateExecution execution) throws Exception
    {
        // Create the request.
        EmailSendRequest emailSendRequest = new EmailSendRequest();

        // Get expression variables from the execution.
        emailSendRequest.setTo(activitiHelper.getExpressionVariableAsString(to, execution));
        emailSendRequest.setCc(activitiHelper.getExpressionVariableAsString(cc, execution));
        emailSendRequest.setBcc(activitiHelper.getExpressionVariableAsString(bcc, execution));
        emailSendRequest.setSubject(activitiHelper.getRequiredExpressionVariableAsString(subject, execution, "subject"));
        emailSendRequest.setText(activitiHelper.getExpressionVariableAsString(text, execution));
        emailSendRequest.setReplyTo(activitiHelper.getExpressionVariableAsString(replyTo, execution));

        // Call the activitiSesService to send the email.
        activitiSesService.sendEmail(emailSendRequest);
    }
}
