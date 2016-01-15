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
module.exports = function(grunt)
{
    'use strict';

    /*
     * Constants for libraries.
     */
    var lib_underscore = 'bower_components/underscore/underscore.js';
    var lib_moment = 'bower_components/moment/moment.js';
    var lib_jquery = 'bower_components/jquery/dist/jquery.js';
    var lib_angular = 'bower_components/angular/angular.js';
    var lib_angular_route = 'bower_components/angular-route/angular-route.js';
    var lib_angular_mocks = 'bower_components/angular-mocks/angular-mocks.js';
    var lib_ui_bootstrap = 'bower_components/angular-bootstrap/ui-bootstrap-tpls.js';

    require('load-grunt-tasks')(grunt);

    grunt.initConfig({

        /*
         * JSHint configurations.
         * 
         * Delegates options to '.jshintrc' file.
         */
        jshint : {
            main : {
                options : {
                    jshintrc : true
                },
                files : {
                    src : [ 'main/**/*.js', 'test/**/*.js' ]
                }
            }
        },

        csslint : {
            main : {
                options : {
                    csslintrc : '.csslintrc'
                },
                files : {
                    src : [ 'main/styles/*.css' ]
                }
            }
        },

        /*
         * Karma test-runner configuration.
         */
        karma : {
            main : {
                configFile : 'karma.conf.js'
            }
        },

        cssmin : {
            main : {
                src : [ 'main/styles/herd.css' ],
                dest : 'build/dist/herd.min.css'
            }
        },

        /*
         * Generate angular $templateCache from source HTMLs. Minify HTML.
         */
        ngtemplates : {
            main : {
                options : {
                    module : 'herd',
                    htmlmin : {
                        removeComments : true,
                        collapseWhitespace : true,
                        collapseBooleanAttributes : true,
                        removeAttributeQuotes : true,
                        removeRedundantAttributes : true
                    }
                },
                src : [ 'main/templates/*.html' ],
                dest : 'build/temp/templates.js'
            }
        },

        /*
         * Make angular's dependency injection minification-safe. Concatenate
         * source files.
         */
        ngAnnotate : {
            main : {
                src : [
                    lib_jquery,
                    lib_moment,
                    lib_underscore,
                    lib_angular,
                    lib_angular_route,
                    lib_ui_bootstrap,
                    'main/module.js',
                    'main/**/*.js',
                    'build/temp/templates.js' ],
                dest : 'build/dist/herd.js'
            }
        },

        /*
         * Obfuscate and generate source map.
         */
        uglify : {
            main : {
                options : {
                    sourceMap : true,
                    sourceMapName : 'build/dist/herd.min.map'
                },
                src : [ 'build/dist/herd.js' ],
                dest : 'build/dist/herd.min.js'
            }
        },

        clean : {
            main : {
                src : [ 'coverage', 'dist', 'temp' ]
            }
        }
    });

    grunt.registerTask('test', [ 'jshint', 'csslint', 'ngtemplates', 'karma' ]);
    grunt.registerTask('build', [ 'jshint', 'csslint', 'cssmin', 'ngtemplates', 'ngAnnotate', 'uglify' ]);
};