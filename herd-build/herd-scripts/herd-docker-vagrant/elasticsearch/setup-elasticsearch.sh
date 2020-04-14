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

# setup for elasticache locally

set -ex ; 
curl https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.0.1-amd64.deb  > esearch.deb ; 
apt-get install -y ./esearch.deb ; 
rm -f ./esearch.deb ;
echo "network.host : 0.0.0.0" >> /etc/elasticsearch/elasticsearch.yml
echo "http.port : 9200"  >> /etc/elasticsearch/elasticsearch.yml
echo "node.name: node-1" >> /etc/elasticsearch/elasticsearch.yml
echo "cluster.initial_master_nodes: [\"node-1\"]" >> /etc/elasticsearch/elasticsearch.yml
# the below is in the Cloudformation, but doing it here causes things to bork
#echo "script.engine.groovy.inline.search: on" >> /etc/elasticsearch/elasticsearch.yml
