package org.finra.herd.dao;

import org.apache.commons.codec.digest.DigestUtils;

public class S3BatchManifest
{
    private String format;
    private String[] fields;
    private String content;
    private String eTag;
    private String filename;
    private String arn;

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
        if (content == null) eTag = null;
        else if (content.isEmpty()) eTag = "";
        else eTag = DigestUtils.md5Hex(content);

        this.content = content;
    }

    public String getETag()
    {
        return eTag;
    }

    public void setFilename(String filename) { this.filename = filename; }

    public String getFilename() { return filename; }

    public void setArn(String arn) { this.arn = arn; }

    public String getArn() { return arn; }
}
