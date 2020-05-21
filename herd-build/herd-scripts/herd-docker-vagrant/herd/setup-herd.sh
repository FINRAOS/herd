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

# rather than deal with cumbersome docker exec, put it in a script
# and it'll work everywhere


herdTagVersion=${HERD_RELEASE}
herdUIVersion='0.89.0'
NexusPath='https://oss.sonatype.org/service/local/repositories/releases/content/org/finra/herd/'

# Tomcat paths since they change based on whether we're using the apache tomcat image
# or rolled our own

TC_HOME=/usr/local/tomcat

set -ex ;

# clear out existing apps
rm -rf $TC_HOME/webapps/*
mv /herd-app.war $TC_HOME/webapps/
#curl ${NexusPath}herd-war/${herdTagVersion}/herd-war-${herdTagVersion}.war > $TC_HOME/webapps/herd.war
#chown tomcat8:tomcat8 $TC_HOME/webapps/herd.war
chmod 0664 $TC_HOME/webapps/herd-app.war

curl 'https://jdbc.postgresql.org/download/postgresql-9.4-1202.jdbc41.jar' > $TC_HOME/lib/postgresql-9.4-1202.jdbc41.jar
#chown tomcat8:tomcat8 $TC_HOME/lib/postgresql-9.4-1202.jdbc41.jar
chmod 0644 $TC_HOME/lib/postgresql-9.4-1202.jdbc41.jar

cat > $TC_HOME/conf/context.xml << EOF
<?xml version="1.0"?>
<Context>
  <WatchedResource>WEB-INF/web.xml</WatchedResource>
  <ResourceLink name="jdbc/herdDB" global="jdbc/herdDB" type="javax.sql.DataSource" />
</Context>

EOF


cat > $TC_HOME/conf/server.xml << EOF2
<?xml version='1.0' encoding='utf-8'?>
<Server port="8005" shutdown="SHUTDOWN">
  <Listener className="org.apache.catalina.startup.VersionLoggerListener" />
  <!-- Security listener. Documentation at /docs/config/listeners.html
  <Listener className="org.apache.catalina.security.SecurityListener" />
  -->
  <!--APR library loader. Documentation at /docs/apr.html -->
  <Listener className="org.apache.catalina.core.AprLifecycleListener" SSLEngine="on" />
  <!-- Prevent memory leaks due to use of particular java/javax APIs-->
  <Listener className="org.apache.catalina.core.JreMemoryLeakPreventionListener" />
  <Listener className="org.apache.catalina.mbeans.GlobalResourcesLifecycleListener" />
  <Listener className="org.apache.catalina.core.ThreadLocalLeakPreventionListener" />

  <GlobalNamingResources>
        <Resource name="jdbc/herdDB"
                   url="jdbc:postgresql://herd-pgsql:5432/herd"
                   username="finraherd"
                   password="Changeme123"
                   auth="Container"
                   driverClassName="org.postgresql.Driver"
                   factory="org.apache.commons.dbcp.BasicDataSourceFactory"
                   initialSize="2"
                   jmxEnabled="true"
                   logAbandoned="true"
                   maxActive="200"
                   maxIdle="200"
                   maxWait="10000"
                   minEvictableIdleTimeMillis="60000"
                   minIdle="10"
                   removeAbandoned="true"
                   removeAbandonedTimeout="885"
                   testOnBorrow="true"
                   testOnReturn="false"
                   testWhileIdle="true"
                   timeBetweenEvictionRunsMillis="5000"
                   type="javax.sql.DataSource"
                   validationInterval="30000"
                   validationQuery="SELECT 1"/>
    <!-- Editable user database that can also be used by
         UserDatabaseRealm to authenticate users
    -->
    <Resource name="UserDatabase" auth="Container"
              type="org.apache.catalina.UserDatabase"
              description="User database that can be updated and saved"
              factory="org.apache.catalina.users.MemoryUserDatabaseFactory"
              pathname="conf/tomcat-users.xml" />
  </GlobalNamingResources>

  <!-- A "Service" is a collection of one or more "Connectors" that share
       a single "Container" Note:  A "Service" is not itself a "Container",
       so you may not define subcomponents such as "Valves" at this level.
       Documentation at /docs/config/service.html
   -->
  <Service name="Catalina">

    <!--The connectors can use a shared executor, you can define one or more named thread pools-->
    <!--
    <Executor name="tomcatThreadPool" namePrefix="catalina-exec-"
        maxThreads="150" minSpareThreads="4"/>
    -->


    <!-- A "Connector" represents an endpoint by which requests are received
         and responses are returned. Documentation at :
         Java HTTP Connector: /docs/config/http.html (blocking & non-blocking)
         Java AJP  Connector: /docs/config/ajp.html
         APR (HTTP/AJP) Connector: /docs/apr.html
         Define a non-SSL/TLS HTTP/1.1 Connector on port 8080
    -->
    <Connector port="8080" protocol="HTTP/1.1"
               connectionTimeout="20000"
               redirectPort="8443" />
    <!-- A "Connector" using the shared thread pool-->
    <!--
    <Connector executor="tomcatThreadPool"
               port="8080" protocol="HTTP/1.1"
               connectionTimeout="20000"
               redirectPort="8443" />
    -->
    <!-- Define a SSL/TLS HTTP/1.1 Connector on port 8443
         This connector uses the NIO implementation that requires the JSSE
         style configuration. When using the APR/native implementation, the
         OpenSSL style configuration is required as described in the APR/native
         documentation -->
    <!--
    <Connector port="8443" protocol="org.apache.coyote.http11.Http11NioProtocol"
               maxThreads="150" SSLEnabled="true" scheme="https" secure="true"
               clientAuth="false" sslProtocol="TLS" />
    -->

    <!-- Define an AJP 1.3 Connector on port 8009 -->
    <Connector port="8009" protocol="AJP/1.3" redirectPort="8443" />


    <!-- An Engine represents the entry point (within Catalina) that processes
         every request.  The Engine implementation for Tomcat stand alone
         analyzes the HTTP headers included with the request, and passes them
         on to the appropriate Host (virtual host).
         Documentation at /docs/config/engine.html -->

    <!-- You should set jvmRoute to support load-balancing via AJP ie :
    <Engine name="Catalina" defaultHost="localhost" jvmRoute="jvm1">
    -->
    <Engine name="Catalina" defaultHost="localhost">

      <!--For clustering, please take a look at documentation at:
          /docs/cluster-howto.html  (simple how to)
          /docs/config/cluster.html (reference documentation) -->
      <!--
      <Cluster className="org.apache.catalina.ha.tcp.SimpleTcpCluster"/>
      -->

      <!-- Use the LockOutRealm to prevent attempts to guess user passwords
           via a brute-force attack -->
      <Realm className="org.apache.catalina.realm.LockOutRealm">
        <!-- This Realm uses the UserDatabase configured in the global JNDI
             resources under the key "UserDatabase".  Any edits
             that are performed against this UserDatabase are immediately
             available for use by the Realm.  -->
        <Realm className="org.apache.catalina.realm.UserDatabaseRealm"
               resourceName="UserDatabase"/>
      </Realm>

      <Host name="localhost"  appBase="webapps"
            unpackWARs="true" autoDeploy="true">

        <!-- SingleSignOn valve, share authentication between web applications
             Documentation at: /docs/config/valve.html -->
        <!--
        <Valve className="org.apache.catalina.authenticator.SingleSignOn" />
        -->

        <!-- Access log processes all example.
             Documentation at: /docs/config/valve.html
             Note: The pattern used is equivalent to using pattern="common" -->
        <Valve className="org.apache.catalina.valves.AccessLogValve" directory="logs"
               prefix="localhost_access_log" suffix=".txt"
               pattern="%h %l %u %t &quot;%r&quot; %s %b" />

      </Host>
    </Engine>
  </Service>
  </Server>

EOF2

cat > $TC_HOME/conf/tomcat-users.xml << EOF3
<tomcat-users xmlns="http://tomcat.apache.org/xml"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
              xsi:schemaLocation="http://tomcat.apache.org/xml tomcat-users.xsd"
              version="1.0">
<!--
  NOTE:  By default, no user is included in the "manager-gui" role required
  to operate the "/manager/html" web application.  If you wish to use this app,
  you must define such a user - the username and password are arbitrary. It is
  strongly recommended that you do NOT use one of the users in the commented out
  section below since they are intended for use with the examples web
  application.
-->
<!--
  NOTE:  The sample user and role entries below are intended for use with the
  examples web application. They are wrapped in a comment and thus are ignored
  when reading this file. If you wish to configure these users for use with the
  examples web application, do not forget to remove the <!.. ..> that surrounds
  them. You will also need to set the passwords to something appropriate.
-->
<!--
  <role rolename="tomcat"/>
  <role rolename="role1"/>
  <user username="tomcat" password="<must-be-changed>" roles="tomcat"/>
  <user username="both" password="<must-be-changed>" roles="tomcat,role1"/>
  <user username="role1" password="<must-be-changed>" roles="role1"/>
-->
  <role rolename="test1"/>
  <user username="test1" password="test123" roles="test1"/>
</tomcat-users>
EOF3

## NOTE: CORS settings can be finicky, so if this needs to get hacked up for making
## the docker container run elsewhere other than localhost

/bin/sed -i '/Built In Filter Definitions/a \
<filter>\
<filter-name>CorsFilter</filter-name>\
<filter-class>org.apache.catalina.filters.CorsFilter</filter-class>\
<init-param>\
<param-name>cors.allowed.origins</param-name>\
<param-value>http://localhost:5443</param-value>\
</init-param>\
<init-param>\
<param-name>cors.allowed.headers</param-name>\
<param-value>Content-Type,Authorization,Accept,Origin</param-value>\
</init-param>\
<init-param>\
<param-name>cors.allowed.methods</param-name>\
<param-value>GET,POST,PUT,DELETE,HEAD,OPTIONS</param-value>\
</init-param>\
<init-param>\
<param-name>cors.support.credentials</param-name>\
<param-value>true</param-value>\
</init-param>\
</filter>\
<filter-mapping>\
<filter-name>CorsFilter</filter-name>\
<url-pattern>/*</url-pattern>\
</filter-mapping>' $TC_HOME/conf/web.xml


/bin/sed -i '/Options Indexes FollowSymLinks/a RewriteRule ^ index.html [L]' /etc/apache2/apache2.conf
/bin/sed -i '/Options Indexes FollowSymLinks/a RewriteRule ^ - [L]' /etc/apache2/apache2.conf
/bin/sed -i '/Options Indexes FollowSymLinks/a RewriteCond %{REQUEST_FILENAME} -d' /etc/apache2/apache2.conf
/bin/sed -i '/Options Indexes FollowSymLinks/a RewriteCond %{REQUEST_FILENAME} -f [OR]' /etc/apache2/apache2.conf
/bin/sed -i '/Options Indexes FollowSymLinks/a RewriteEngine On' /etc/apache2/apache2.conf
#sudo service apache2 start
# Deployig herd-ui
curl https://registry.npmjs.org/@herd/herd-ui-dist/-/herd-ui-dist-${herdUIVersion}.tgz | /bin/tar xz -C /tmp
mv /tmp/package/dist/* /var/www/html
# Configuring directory and file ownership for the http server
#sudo chown -R httpd:httpd /var/www
# for debian docker images, need to enable mod_rewrite
pushd /etc/apache2/mods-enabled
ln -s ../mods-available/rewrite.* .
popd
# make permissions sane
chmod 2775 /var/www
find /var/www -type d -exec chmod 2775 {} + 
find /var/www -type f -exec chmod 0664 {} + 
# Setup configuration.json herd-ui settings
# moved to run-herd script


