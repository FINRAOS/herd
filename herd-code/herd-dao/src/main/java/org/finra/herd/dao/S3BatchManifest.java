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
package org.finra.herd.dao;

import org.apache.commons.codec.digest.DigestUtils;


//TODO: Move this dude to dto with extra helper for content and location arn
public class S3BatchManifest
{
    private String format;
    private String[] fields;
    private String content;
    private String etag;
    private String key;
    private String bucketName;

    public String getFormat()
    {
        return format;
    }

    public void setFormat(String format)
    {
        this.format = format;
    }

    public String[] getFields()
    {
        return fields;
    }

    public void setFields(String[] fields)
    {
        this.fields = fields;
    }

    public String getContent()
    {
        return content;
    }

    /**
     * Set the content value and calculate etag for given content
     *
     * @param content manifest file content
     */
    public void setContent(String content)
    {
        if (content == null)
        {
            etag = null;
        }
        else if (content.isEmpty())
        {
            etag = "";
        }
        else
        {
            etag = DigestUtils.md5Hex(content);
        }

        this.content = content;
    }

    public String getETag()
    {
        return etag;
    }

    public void setKey(String key)
    {
        this.key = key;
    }

    public String getS3Key()
    {
        return key;
    }

    public String getBucketName()
    {
        return bucketName;
    }

    public void setBucketName(String bucketName)
    {
        this.bucketName = bucketName;
    }

    public String getLocationArn()
    {
        return String.format("arn:aws:s3:::%s/%s", bucketName, key);
    }
}
