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
package org.finra.herd.service.helper;

import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import org.finra.herd.model.api.xml.S3PropertiesLocation;

@Component
public class S3PropertiesLocationHelper
{
    /**
     * Validates the given {@link S3PropertiesLocation}.
     * A {@link S3PropertiesLocation} is valid when:
     * <ol>
     * <li>bucket name is not blank, and;</li>
     * <li>object key is not blank.</li>
     * </ol>
     * Throws a {@link IllegalArgumentException} when validation fails.
     * The bucket name and key are trimmed as a side-effect.
     * 
     * @param s3PropertiesLocation {@link S3PropertiesLocation}
     */
    public void validate(S3PropertiesLocation s3PropertiesLocation)
    {
        String bucketName = s3PropertiesLocation.getBucketName();
        String key = s3PropertiesLocation.getKey();

        Assert.hasText(bucketName, "S3 properties location bucket name must be specified.");
        Assert.hasText(key, "S3 properties location object key must be specified.");

        s3PropertiesLocation.setBucketName(bucketName.trim());
        s3PropertiesLocation.setKey(key.trim());
    }
}
