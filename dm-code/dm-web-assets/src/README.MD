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

# DM Web Assets

## Directory manifest

- assembly/
    - Contains maven assembly configuration to package this mvn module as a zip and copy it to the war module.
- bower_components/
    - Contains the browser dependencies downloaded by bower.
    - This should normally not be committed, but due to enterprise firewall, we cannot download from github repo directly from our build server.
- main/
    - Contains the product source code for DM-UI component.
- static_assets/
    - Contains static assets (images, css, js, html). Most of these assets belong to activiti UI.
- test/
    - Contains unit tests and integration tests for the sources in main/
- bower.json
    - bower package configuration.
    - Contains list of browser dependencies which can be downloaded via bower.
- Gruntfile.js
    - grunt-cli configuration.
    - Contains test and build process.
- package.json
    - node package configuration.
    - Contains dev-dependencies for building UI.

## Installation

    npm install

## Unit testing and generating code coverage

After installation:

    npm run test

Generates a directory build/
- build/coverage
    - Contains coverage results for each browser that the test was configured to run in karma. Generates a index.html for human readable results, and a cobertura-coverage.xml for machine readable output.
- build/test-results.xml
    - A junit output of the unit tests. Used for machine readable output.

## Minifying js

After installation:

    npm run build

Generates a directory build/dist
- dm.min.js is the minified output of the entire project.
- dm.js is the non-obfuscated output, which the source map dm.min.map depends on.
- dm.min.css is the minified CSS output.