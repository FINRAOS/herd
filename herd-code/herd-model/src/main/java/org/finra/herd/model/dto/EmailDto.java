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
package org.finra.herd.model.dto;

/**
 * Complete representation of an Email.
 */
public class EmailDto
{

    /**
     * Builder which defines required/optional fields which fully represent an email.
     */
    public static class Builder
    {

        private String to;
        private String cc;
        private String bcc;
        private String subject;
        private String text;
        private String html;
        private String replyTo;

        public Builder(String to)
        {
            this.to = to;
        }

        public EmailDto build()
        {
            EmailDto emailDto = new EmailDto(to);
            emailDto.cc = cc;
            emailDto.bcc = bcc;
            emailDto.subject = subject;
            emailDto.text = text;
            emailDto.html = html;
            emailDto.replyTo = replyTo;

            return emailDto;
        }

        public Builder withCc(String cc)
        {
            this.cc = cc;
            return this;
        }

        public Builder withBcc(String bcc)
        {
            this.bcc = bcc;
            return this;
        }

        public Builder withSubject(String subject)
        {
            this.subject = subject;
            return this;
        }

        public Builder withText(String text)
        {
            this.text = text;
            return this;
        }

        public Builder withHtml(String html)
        {
            this.html = html;
            return this;
        }

        public Builder withReplyTo(String replyTo)
        {
            this.replyTo = replyTo;
            return this;
        }

    }

    private String to;
    private String cc;
    private String bcc;
    private String subject;
    private String text;
    private String html;
    private String replyTo;

    private EmailDto(String to)
    {
        this.to = to;
    }

    public String getTo()
    {
        return to;
    }

    public String getCc()
    {
        return cc;
    }

    public String getBcc()
    {
        return bcc;
    }

    public String getSubject()
    {
        return subject;
    }

    public String getText()
    {
        return text;
    }

    public String getHtml()
    {
        return html;
    }

    public String getReplyTo()
    {
        return replyTo;
    }

    @Override
    public String toString()
    {
        return "EmailDto{" + "to='" + to + '\'' + ", cc='" + cc + '\'' + ", bcc='" + bcc + '\'' + ", subject='" + subject + '\'' + ", text='" + text + '\'' +
            ", html='" + html + '\'' + ", replyTo=" + replyTo + '}';
    }
}
