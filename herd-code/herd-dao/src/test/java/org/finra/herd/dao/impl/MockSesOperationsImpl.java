package org.finra.herd.dao.impl;

import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;

import org.finra.herd.dao.SesOperations;

public class MockSesOperationsImpl implements SesOperations
{
    @Override
    public void sendEmail(SendEmailRequest sendEmailRequest, AmazonSimpleEmailService simpleEmailService)
    {

    }
}
