#!/bin/bash
# Copyright 2015 herd contributors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# need to start apache first and then run catalina
# the way the container did before

# FIRST RUN

if [ ! -e /var/www/html/configuration.json ] ; then

	echo "Determining host IP for configuration ..."
	if [ -z $HERD_UI_HOST ] ; then 
		# check if we're in AWS, or bork
		herd_ui_host=$(/usr/bin/curl http://169.254.169.254/latest/meta-data/public-hostname)
		if [ -z herd_ui_host ] ; then
			echo "ERROR: unable to determine host IP or name; please set HERD_UI_HOST env variable for container."
			exit 2
		fi;

	else
		herd_ui_host=$HERD_UI_HOST
	fi;
	echo "Setting UI host to $herd_ui_host"

echo > /var/www/html/configuration.json << EOF3
{
  'restBaseUri': 'http://'$herd_ui_host':8080/herd-app/rest',
  'basicAuthRestBaseUri': 'basicAuthRestBaseUri',
  'helpUrl': 'helpUrl',
  'supportEmail': 'orgSupportEmail',
  'brandHeader': 'Herd-UI',
  'brandMotto': 'Locate and understand data available in HERD',
  'docTitlePrefix': 'Herd-UI',
  'useBasicAuth': 'false',
  'alertDelayInSeconds': '10',
  'trackAnalytics': 'false',
  'ga': {
    'key': 'key',
    'iv': 'iv',
    'trackingId': 'trackingId'
  }
}

EOF3

fi ;


service apache2 start
catalina.sh run
