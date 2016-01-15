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
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE HTML>
<html>

<head>
   <title>Build Information</title>
   <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate"/>
   <meta http-equiv="Pragma" content="no-cache"/>
   <meta http-equiv="Expires" content="0"/>
   <%-- This tag will try to force the browser to use the latest rendering mode the browser supports.--%>
   <meta http-equiv="X-UA-Compatible" content="IE=edge"/>
   <meta http-equiv="content-type" content="text/html; charset=UTF-8"/>
</head>

<body style="overflow: hidden">

Build Information
<br/><br/>
Build Number: ${buildInformation.buildNumber}<br/>
Build User: ${buildInformation.buildUser}<br/>
Build Time: ${buildInformation.buildDate}<br/>
Build OS: ${buildInformation.buildOs}<br/>

</body>

</html>
