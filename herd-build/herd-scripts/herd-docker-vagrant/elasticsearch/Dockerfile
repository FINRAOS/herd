# vim:set ft=dockerfile:
#FROM debian:stretch-slim
FROM openjdk:8-jdk

ADD setup-elasticsearch.sh /setup/setup-elasticsearch.sh
RUN set -ex ; \
	chmod a+x /setup/setup-elasticsearch.sh ; cd /setup ; bash ./setup-elasticsearch.sh
EXPOSE 9200 9300
USER elasticsearch
ENTRYPOINT ["/usr/share/elasticsearch/bin/elasticsearch"]
