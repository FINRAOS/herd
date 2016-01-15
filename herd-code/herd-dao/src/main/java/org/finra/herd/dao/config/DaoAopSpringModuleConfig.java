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
package org.finra.herd.dao.config;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import org.finra.herd.dao.helper.AwsEc2ExceptionRetryAdvice;
import org.finra.herd.dao.helper.AwsEmrExceptionRetryAdvice;
import org.finra.herd.dao.helper.AwsS3ExceptionRetryAdvice;
import org.finra.herd.dao.helper.AwsSqsExceptionRetryAdvice;
import org.finra.herd.dao.helper.AwsStsExceptionRetryAdvice;

/**
 * Dao AOP Spring module configuration. This class defines specific configuration related to aspects.
 */
@Configuration
@EnableAspectJAutoProxy
@Aspect
public class DaoAopSpringModuleConfig
{
    @Autowired
    private AwsS3ExceptionRetryAdvice awsS3ExceptionRetryAdvice;

    @Autowired
    private AwsEmrExceptionRetryAdvice awsEmrExceptionRetryAdvice;

    @Autowired
    private AwsEc2ExceptionRetryAdvice awsEc2ExceptionRetryAdvice;

    @Autowired
    private AwsStsExceptionRetryAdvice awsStsExceptionRetryAdvice;

    @Autowired
    private AwsSqsExceptionRetryAdvice awsSqsExceptionRetryAdvice;

    /**
     * A pointcut for S3 operations methods.
     */
    @Pointcut("execution(* org.finra.herd.dao.S3Operations.*(..))")
    public void s3OperationsMethods()
    {
        // Pointcut methods are defined by their annotation and don't have an implementation.
    }

    /**
     * A pointcut for EMR operations methods.
     */
    @Pointcut("execution(* org.finra.herd.dao.EmrOperations.*(..))")
    public void emrOperationsMethods()
    {
        // Pointcut methods are defined by their annotation and don't have an implementation.
    }

    /**
     * A pointcut for EC2 operations methods.
     */
    @Pointcut("execution(* org.finra.herd.dao.Ec2Operations.*(..))")
    public void ec2OperationsMethods()
    {
        // Pointcut methods are defined by their annotation and don't have an implementation.
    }

    /**
     * A pointcut for STS operations methods.
     */
    @Pointcut("execution(* org.finra.herd.dao.StsOperations.*(..))")
    public void stsOperationsMethods()
    {
        // Pointcut methods are defined by their annotation and don't have an implementation.
    }

    /**
     * A pointcut for SQS operations methods.
     */
    @Pointcut("execution(* org.finra.herd.dao.SqsOperations.*(..))")
    public void sqsOperationsMethods()
    {
        // Pointcut methods are defined by their annotation and don't have an implementation.
    }

    /**
     * Around advice that catches AWS S3 throttling exceptions and retries a configurable amount of time.
     *
     * @param pjp the proceeding join point.
     *
     * @return the return value of the method we are advising.
     * @throws Throwable if there were any problems executing the method.
     */
    @Around("s3OperationsMethods()")
    public Object retryOnException(ProceedingJoinPoint pjp) throws Throwable
    {
        return awsS3ExceptionRetryAdvice.retryOnException(pjp);
    }

    /**
     * Around advice that catches AWS EMR throttling exceptions and retries a configurable amount of time.
     *
     * @param pjp the proceeding join point.
     *
     * @return the return value of the method we are advising.
     * @throws Throwable if there were any problems executing the method.
     */
    @Around("emrOperationsMethods()")
    public Object retryOnEmrException(ProceedingJoinPoint pjp) throws Throwable
    {
        return awsEmrExceptionRetryAdvice.retryOnException(pjp);
    }

    /**
     * Around advice that catches AWS EC2 throttling exceptions and retries a configurable amount of time.
     *
     * @param pjp the proceeding join point.
     *
     * @return the return value of the method we are advising.
     * @throws Throwable if there were any problems executing the method.
     */
    @Around("ec2OperationsMethods()")
    public Object retryOnEc2Exception(ProceedingJoinPoint pjp) throws Throwable
    {
        return awsEc2ExceptionRetryAdvice.retryOnException(pjp);
    }

    /**
     * Around advice that catches AWS STS throttling exceptions and retries a configurable amount of time.
     *
     * @param pjp the proceeding join point.
     *
     * @return the return value of the method we are advising.
     * @throws Throwable if there were any problems executing the method.
     */
    @Around("stsOperationsMethods()")
    public Object retryOnStsException(ProceedingJoinPoint pjp) throws Throwable
    {
        return awsStsExceptionRetryAdvice.retryOnException(pjp);
    }

    /**
     * Around advice that catches AWS SQS throttling exceptions and retries a configurable amount of time.
     *
     * @param pjp the proceeding join point.
     *
     * @return the return value of the method we are advising.
     * @throws Throwable if there were any problems executing the method.
     */
    @Around("sqsOperationsMethods()")
    public Object retryOnSqsException(ProceedingJoinPoint pjp) throws Throwable
    {
        return awsSqsExceptionRetryAdvice.retryOnException(pjp);
    }
}
