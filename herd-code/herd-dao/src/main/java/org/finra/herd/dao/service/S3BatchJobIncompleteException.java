package org.finra.herd.dao.service;

import com.amazonaws.services.s3control.model.DescribeJobResult;

public class S3BatchJobIncompleteException extends RuntimeException
{
    DescribeJobResult jobDescriptor = null;

    public S3BatchJobIncompleteException() {
        super();
    }

    public S3BatchJobIncompleteException(DescribeJobResult jobDescriptor) {
        super();
        this.jobDescriptor = jobDescriptor;
    }

    @Override
    public String toString()
    {
        return "S3BatchJobIncompleteException{" + "jobDescriptor=" + jobDescriptor + '}';
    }
}


