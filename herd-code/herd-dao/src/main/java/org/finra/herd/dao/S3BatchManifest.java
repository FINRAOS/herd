package org.finra.herd.dao;

import org.apache.commons.codec.digest.DigestUtils;

public class S3BatchManifest
{
    private String format;
    private String[] fields;
    private String content;
    private String eTag;
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

    public void setContent(String content)
    {
        if (content == null)
        {
            eTag = null;
        }
        else if (content.isEmpty())
        {
            eTag = "";
        }
        else
        {
            eTag = DigestUtils.md5Hex(content);
        }

        this.content = content;
    }

    public String getETag()
    {
        return eTag;
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
