-------------------------------------------------------------------------
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
-------------------------------------------------------------------------


This project builds Swagger documentation and packages it up as a Zip archive.
The majority of the files checked into this Maven sub-project should ultimately be obtained via Github when they become available.
For now, the following process has been followed:

o The src/assembly directory contains assets.xml which provide the rules for how to zip up the final archive. This remains moving forward.
o The src/main/dm.swagger.yaml file is the actual DM documentation that gets read in by the swagger-ui page. This remains moving forward.
o swagger-ui-master is the customized version of swagger-ui that Taras created. These customizations allow for a more customized UI set of
  functionality (e.g. XML Example). The majority of this folder should eventually come from Github. For now, the following was done to
  produce what is currently checked in:
  o Download custom swagger-ui version from https://github.com/tkatkov/swagger-ui/tree/wide and place it in dm-docs-swagger/src/swagger-ui-master.
  o dm-docs-swagger/src/swagger-ui-master/src/main/html/index.html was modified to be custom for FINRA. Validation has been removed and the petstore
    URL is modified to point to "../dm.swagger.yaml". This file should ultimately move to another part of the Maven project (maybe src/main).
  o To get the custom swagger client build (i.e. swagger-js), the latest version of swagger-js (https://github.com/swagger-api/swagger-js)
    was downloaded and the browser directory was added to dm-docs-swagger/src/swagger-ui-master/node_modules/swagger-client.
    This should be removed once the next version of swagger-js is officially released.
  o Delete the following line from swagger-ui-master/package.json: "http-server": "git+https://github.com/nodeapps/http-server.git"
    since it points to an external git repo that may not be accessbile due to firewall rules. It is only used for testing out the swagger-ui
    project which we don't need to do as part of our documentation build.
  o Delete "node_modules" entry from dm-docs-swagger\src\swagger-ui-master\.gitignore so it can be checked into Git.

