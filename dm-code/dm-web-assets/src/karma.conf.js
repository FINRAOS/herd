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
module.exports = function(config)
{/*
     * Constants for libraries.
     */
    var lib_underscore = 'bower_components/underscore/underscore.js';
    var lib_moment = 'bower_components/moment/moment.js';
    var lib_jquery = 'bower_components/jquery/dist/jquery.js';
    var lib_angular = 'bower_components/angular/angular.js';
    var lib_angular_route = 'bower_components/angular-route/angular-route.js';
    var lib_angular_mocks = 'bower_components/angular-mocks/angular-mocks.js';
    var lib_ui_bootstrap = 'bower_components/angular-bootstrap/ui-bootstrap-tpls.js';

    config.set({

        frameworks : [ 'jasmine' ], // use Jasmine for testing
        browsers : [ 'PhantomJS' ], // run tests in PhantomJS
        singleRun : true, // run once and shutdown
        /*
         * Files to include for testing. Order of inclusion: Libraries, Module,
         * Other source JS, Testing libraries, Test JS files
         */
        files : [
            lib_jquery,
            lib_moment,
            lib_underscore,
            lib_angular,
            lib_angular_route,
            lib_ui_bootstrap,
            'main/module.js',
            'main/**/*.js',
            'build/temp/templates.js',
            lib_angular_mocks,
            'test/**/*.js' ],

        reporters : [ 'mocha', 'junit', 'coverage' ],

        junitReporter : {
            outputFile : 'build/test-results.xml'
        },

        /*
         * Coverage configurations
         */
        /*
         * Preprocess source files for instrumentation
         */
        preprocessors : {
            'main/**/*.js' : [ 'coverage' ]
        },
        /*
         * Output coverage results as HTML and cobertura
         */
        coverageReporter : {
            reporters : [ {
                type : 'html',
                dir : 'build/coverage'
            }, {
                type : 'cobertura',
                dir : 'build/coverage'
            } ]
        },

        // avoid timing out the server for some long running tests
        browserNoActivityTimeout : 60000
    });
};