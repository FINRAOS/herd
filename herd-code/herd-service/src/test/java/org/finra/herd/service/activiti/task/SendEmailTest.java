package org.finra.herd.service.activiti.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.activiti.bpmn.model.FieldExtension;
import org.junit.Test;

import org.finra.herd.model.api.xml.Parameter;
import org.finra.herd.service.activiti.ActivitiRuntimeHelper;

/**
 * Tests the SendEmail Activiti task wrapper.
 */
public class SendEmailTest extends HerdActivitiServiceTaskTest
{
    /**
     * This method tests the send email activiti task
     */
    @Test
    public void testSendEmail() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("bcc", "${bcc}"));
        fieldExtensionList.add(buildFieldExtension("cc", "${cc}"));
        fieldExtensionList.add(buildFieldExtension("replyTo", "${replyTo}"));
        fieldExtensionList.add(buildFieldExtension("subject", "${subject}"));
        fieldExtensionList.add(buildFieldExtension("text", "${text}"));
        fieldExtensionList.add(buildFieldExtension("to", "${to}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("bcc", "test1@abc.com"));
        parameters.add(buildParameter("cc", "test2@abc.com"));
        parameters.add(buildParameter("replyTo", "test5@abc.com"));
        parameters.add(buildParameter("subject", "sample Email"));
        parameters.add(buildParameter("text", "sample test body"));
        parameters.add(buildParameter("to", "test0@abc.com"));

        testActivitiServiceTaskSuccess(SendEmail.class.getCanonicalName(), fieldExtensionList, parameters, new HashMap<>());
    }

    @Test
    public void testSendEmailWithoutSubject() throws Exception
    {
        List<FieldExtension> fieldExtensionList = new ArrayList<>();
        fieldExtensionList.add(buildFieldExtension("bcc", "${bcc}"));
        fieldExtensionList.add(buildFieldExtension("cc", "${cc}"));
        fieldExtensionList.add(buildFieldExtension("replyTo", "${replyTo}"));
        fieldExtensionList.add(buildFieldExtension("text", "${text}"));
        fieldExtensionList.add(buildFieldExtension("to", "${to}"));

        List<Parameter> parameters = new ArrayList<>();
        parameters.add(buildParameter("bcc", "test1@abc.com"));
        parameters.add(buildParameter("cc", "test2@abc.com"));
        parameters.add(buildParameter("replyTo", "test5@abc.com"));
        parameters.add(buildParameter("text", "sample test body"));
        parameters.add(buildParameter("to", "test0@abc.com"));

        Map<String, Object> variableValuesToValidate = new HashMap<>();
        variableValuesToValidate.put(ActivitiRuntimeHelper.VARIABLE_ERROR_MESSAGE, "\"subject\" must be specified.");

        executeWithoutLogging(ActivitiRuntimeHelper.class,
            () -> testActivitiServiceTaskFailure(SendEmail.class.getCanonicalName(), fieldExtensionList, parameters, variableValuesToValidate));
    }
}
