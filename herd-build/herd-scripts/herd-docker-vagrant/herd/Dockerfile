# vim:set ft=dockerfile:
#FROM debian:stretch-slim
FROM tomcat:jre8


ADD setup-os.sh /setup/setup-os.sh
RUN set -ex ; \ 
	chmod a+x /setup/setup-os.sh ; \
	cd /setup/ ; \
	bash /setup/setup-os.sh ;

ADD setup-herd.sh /setup/setup-herd.sh
ARG HERD_RELEASE
ADD herd-war-${HERD_RELEASE}.war /herd-app.war
RUN set -ex ; \
	chmod a+x /setup/setup-herd.sh; \
	cd /setup/ ; \
	bash /setup/setup-herd.sh ;

ADD run-herd.sh /usr/local/tomcat
RUN chmod a+x /usr/local/tomcat/run-herd.sh

ENTRYPOINT /usr/local/tomcat/run-herd.sh

EXPOSE 80 8080

