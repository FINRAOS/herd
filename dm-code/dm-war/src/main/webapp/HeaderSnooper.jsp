<!--
  Copyright 2015 herd contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<%@page import="java.util.Enumeration" %>
<%@ page import="org.apache.log4j.Logger" %>
<%@ page import="org.springframework.web.context.WebApplicationContext" %>
<%@ page import="org.springframework.web.context.support.WebApplicationContextUtils" %>
<%@ page import="org.springframework.web.util.HtmlUtils" %>

<%
   // Determine if the header snooper is enabled.
   // It is disabled by default for security purposes.
   ServletContext servletContext = this.getServletContext();
   WebApplicationContext wac = WebApplicationContextUtils.getRequiredWebApplicationContext(servletContext);
   String headerSnooperEnabledString = wac.getEnvironment().getProperty("header.snooper.enabled");
   boolean headerSnooperEnabled = false;
   if (headerSnooperEnabledString != null)
   {
      try
      {
         headerSnooperEnabled = Boolean.parseBoolean(headerSnooperEnabledString);
      }
      catch (Exception ex)
      {
         // Use default.
      }
   }

   // Log the headers even if we disable the header snooper from the user UI so we can still get the headers for debugging purposes.
   Logger logger = Logger.getLogger("org.finra.dm.HeaderSnooper");
   Enumeration<?> enumeration = request.getHeaderNames();
   while (enumeration.hasMoreElements())
   {
      String key = HtmlUtils.htmlEscape((String) enumeration.nextElement());
      String value = HtmlUtils.htmlEscape(request.getHeader(key));
      logger.info("Key: " + key + ", Value: " + value);
   }
%>
<HTML>
<HEAD><TITLE>Test Web Agent Headers </TITLE></HEAD>
<BODY BGCOLOR=#ffffff>
<%
   if (headerSnooperEnabled)
   {
%>

<TABLE BORDER=1>
   <TR>
      <TD VALIGN=TOP>
         <B>Variable</B>
      </TD>
      <TD VALIGN=TOP>
         <B>Value</B>
      </TD>
   </TR>
   <%
      Enumeration<?> uiEnumeration = request.getHeaderNames();
      while (uiEnumeration.hasMoreElements())
      {
         String key = HtmlUtils.htmlEscape((String) uiEnumeration.nextElement());
         String value = HtmlUtils.htmlEscape(request.getHeader(key));
   %>
   <TR>
      <TD VALIGN=TOP>
         <%=key%>
      </TD>
      <TD VALIGN=TOP>
         <%=value%>
      </TD>
      <%
         }
      %>
   </TR>
</TABLE>

<%
}
else
{
%>
<!-- Module disabled. -->
<%
   }
%>

</BODY>
</HTML>
