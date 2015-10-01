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
package org.finra.dm.service.helper;

import org.junit.Assert;
import org.junit.Test;

import org.finra.dm.model.api.xml.S3PropertiesLocation;
import org.finra.dm.service.AbstractServiceTest;

public class S3PropertiesLocationHelperTest extends AbstractServiceTest
{
    /**
     * validate() throws no errors when {@link S3PropertiesLocation} is given with both bucket name and key as a non-blank string.
     * The given object's bucket name and key should be trimmed as a side effect.
     */
    @Test
    public void testValidateNoErrorsAndTrimmed()
    {
        S3PropertiesLocation s3PropertiesLocation = getS3PropertiesLocation();

        String expectedBucketName = s3PropertiesLocation.getBucketName();
        String expectedKey = s3PropertiesLocation.getKey();

        s3PropertiesLocation.setBucketName(BLANK_TEXT + s3PropertiesLocation.getBucketName() + BLANK_TEXT);
        s3PropertiesLocation.setKey(BLANK_TEXT + s3PropertiesLocation.getKey() + BLANK_TEXT);

        try
        {
            s3PropertiesLocationHelper.validate(s3PropertiesLocation);

            Assert.assertEquals("s3PropertiesLocation bucketName", expectedBucketName, s3PropertiesLocation.getBucketName());
            Assert.assertEquals("s3PropertiesLocation key", expectedKey, s3PropertiesLocation.getKey());
        }
        catch (Exception e)
        {
            Assert.fail("unexpected exception was thrown. " + e);
        }
    }

    /**
     * validate() throws an IllegalArgumentException when bucket name is blank.
     */
    @Test
    public void testValidateWhenBucketNameIsBlankThrowsError()
    {
        S3PropertiesLocation s3PropertiesLocation = getS3PropertiesLocation();
        s3PropertiesLocation.setBucketName(BLANK_TEXT);

        testValidateThrowsError(s3PropertiesLocation, IllegalArgumentException.class, "S3 properties location bucket name must be specified.");
    }

    /**
     * validate() throws an IllegalArgumentException when key is blank.
     */
    @Test
    public void testValidateWhenObjectKeyIsBlankThrowsError()
    {
        S3PropertiesLocation s3PropertiesLocation = getS3PropertiesLocation();
        s3PropertiesLocation.setKey(BLANK_TEXT);

        testValidateThrowsError(s3PropertiesLocation, IllegalArgumentException.class, "S3 properties location object key must be specified.");
    }

    /**
     * Tests that validate() throws an exception with called with the given {@link S3PropertiesLocation}.
     * 
     * @param s3PropertiesLocation {@link S3PropertiesLocation}
     * @param expectedExceptionType expected exception type
     * @param expectedMessage expected exception message
     */
    private void testValidateThrowsError(S3PropertiesLocation s3PropertiesLocation, Class<? extends Exception> expectedExceptionType, String expectedMessage)
    {
        try
        {
            s3PropertiesLocationHelper.validate(s3PropertiesLocation);
            Assert.fail("expected " + expectedExceptionType.getSimpleName() + ", but no exception was thrown");
        }
        catch (Exception e)
        {
            Assert.assertEquals("thrown exception type", expectedExceptionType, e.getClass());
            Assert.assertEquals("thrown exception message", expectedMessage, e.getMessage());
        }
    }

    /**
     * Creates a new {@link S3PropertiesLocation} with bucket name and key.
     * 
     * @return {@link S3PropertiesLocation}
     */
    private S3PropertiesLocation getS3PropertiesLocation()
    {
        S3PropertiesLocation s3PropertiesLocation = new S3PropertiesLocation();
        s3PropertiesLocation.setBucketName("testBucketName");
        s3PropertiesLocation.setKey("testKey");
        return s3PropertiesLocation;
    }
}
