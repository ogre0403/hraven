/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package org.nchc.history;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;
import org.nchc.extend.ExtendConstants;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Simple REST server that spawns an embedded Jetty instance to service requests
 *
 * @deprecated in favor of {@link com.twitter.hraven.rest.HravenRestServer}
 */
public class RestServer extends AbstractIdleService {

  private static final int DEFAULT_PORT = 8080;
  private static final int DEFAULT_SSL_PORT = 443;
  private static final String DEFAULT_ADDRESS = "0.0.0.0";
  private static final String WEBROOT_INDEX = "web";

  private static final Log LOG = LogFactory.getLog(RestServer.class);

  private String address;
  private int port;
  private int sslport;
  private Server server;

//  public RestServer(String address, int port,int sslport) {
//    this.address = address;
//    this.port = port;
//    this.sslport = sslport;
//  }

  public RestServer(Properties ps){
      this.address = ps.getProperty("rest.address",DEFAULT_ADDRESS);
      this.port = Integer.parseInt(ps.getProperty("rest.port", DEFAULT_PORT+""));
      this.sslport = Integer.parseInt(ps.getProperty("rest.ssl.port",DEFAULT_SSL_PORT+""));
      setSuperUser(ps);
      enableHttp(ps);
      enableAuth(ps);
      setRMservers(ps);
      LOG.info("rest.address: " + this.address);
      LOG.info("rest.port: " + this.port);
      LOG.info("rest.ssl.port: " + this.sslport);
  }


    public static void main(String[] args) throws Exception {

        LOG.info("Start TEST http server");
        RestServer rs = new RestServer(DEFAULT_ADDRESS, DEFAULT_PORT, 8443);
        rs.startUp();
        LOG.info("End TEST http server");

    }




    public RestServer(String address, int port, int sslport) {
        this.address = address;
        this.port = port;
        this.sslport = sslport;
        ExtendConstants.isHttpEnable = true;
        ExtendConstants.isAuthEnable = false;

    }


    @Override
    protected void startUp() throws Exception {
        server = new Server();

        //set default max thread number to 250
        QueuedThreadPool threadPool = new QueuedThreadPool();
        server.setThreadPool(threadPool);
        server.setSendServerVersion(false);
        server.setSendDateHeader(false);
        server.setStopAtShutdown(true);


        // sset SSL
        String keystroefile = RestServer.class.getClass().getResource("/keystore").toString();
        SslSocketConnector https_connector = new SslSocketConnector();
        https_connector.setHost(this.address);
        https_connector.setKeystoreType("JKS");
        https_connector.setPort(this.sslport);
        https_connector.setKeystore(keystroefile);
        https_connector.setPassword("123456");
        https_connector.setKeyPassword("123456");
        server.setConnectors(new Connector[]{https_connector});

        // set non-http
        if (ExtendConstants.isHttpEnable == true) {
            Connector http_connector = new SelectChannelConnector();
            http_connector.setPort(this.port);
            http_connector.setHost(this.address);
            server.addConnector(http_connector);
        }


        // static html context, use to get css and js under WEBROOT_INDEX
        String jarpath = RestServer.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        LOG.info("jarpath = " + jarpath);
        Path p1 = Paths.get(jarpath);
        Path p2 = p1.getParent().getParent().resolve(WEBROOT_INDEX);
        LOG.info("web root = " + p2.toUri().toASCIIString());
        WebAppContext wc = new WebAppContext();
        wc.setResourceBase(p2.toUri().toASCIIString());
        wc.setContextPath("/resource");

        // Restful context
        ServletHolder sh = new ServletHolder(ServletContainer.class);
        sh.setInitParameter("com.sun.jersey.config.property.packages", "org.nchc.rest");
        sh.setInitParameter(JSONConfiguration.FEATURE_POJO_MAPPING, "true");
        Context context = new Context(server, "/", Context.SESSIONS);
        context.addServlet(sh, "/*");

        //add web and restful context
        server.setHandlers(new Handler[]{wc, context});
        // start server
        server.start();
    }

    private void setSuperUser(Properties ps) {
        if (ps.containsKey("rest.superuser")) {
            ExtendConstants.SUPERUSER = ps.getProperty("rest.superuser");
        }
        LOG.info("rest.superuser: " + ExtendConstants.SUPERUSER);
    }

    private void enableHttp(Properties ps) {
        if (ps.containsKey("rest.http") &&
                ps.getProperty("rest.http").equals("enable")) {
            ExtendConstants.isHttpEnable = true;
        }
        LOG.info("rest.http: " + ExtendConstants.isHttpEnable);
    }

    private void enableAuth(Properties ps) {
        if (ps.containsKey("rest.auth") &&
                ps.getProperty("rest.auth").equals("enable")) {
            ExtendConstants.isAuthEnable = true;
        }
        LOG.info("rest.auth:" + ExtendConstants.isAuthEnable);
    }

    private void setRMservers(Properties ps){
        if(ps.containsKey("running.yarn.RM_web")){
            ExtendConstants.RMservers[0] = ps.getProperty("running.yarn.RM_web");
            ExtendConstants.RMservers[1] = ps.getProperty("running.yarn.RM_web.backup",ExtendConstants.RMservers[0]);
        }
    }
  @Override
  protected void shutDown() throws Exception {
    server.stop();
  }


}
