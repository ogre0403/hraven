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
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;

import java.net.URI;
import java.net.URL;

/**
 * Simple REST server that spawns an embedded Jetty instance to service requests
 * @deprecated in favor of {@link com.twitter.hraven.rest.HravenRestServer}
 */
public class RestServer extends AbstractIdleService {
  private static final int DEFAULT_PORT = 8080;
  private static final String DEFAULT_ADDRESS = "0.0.0.0";
  private static final String WEBROOT_INDEX = "/webroot/";

  private static final Log LOG = LogFactory.getLog(RestServer.class);

  private final String address;
  private final int port;
  private Server server;

  public RestServer(String address, int port) {
    this.address = address;
    this.port = port;
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

        // set ip and port
        Connector connector = new SelectChannelConnector();
        connector.setPort(this.port);
        connector.setHost(address);
        server.addConnector(connector);

        // static html context
        URL indexUri = this.getClass().getResource(WEBROOT_INDEX);
        URI baseUri = indexUri.toURI();
       // LOG.info("Base URI: " + baseUri);
        WebAppContext wc = new WebAppContext();
        LOG.info("baseUri = " + baseUri.toASCIIString());
        //wc.setResourceBase(baseUri.toASCIIString());
        wc.setResourceBase(".");
        wc.setContextPath("/runJetty");

        // Restful context
        ServletHolder sh = new ServletHolder(ServletContainer.class);
        sh.setInitParameter("com.sun.jersey.config.property.packages", "org.nchc.history");
        sh.setInitParameter(JSONConfiguration.FEATURE_POJO_MAPPING, "true");
        Context context = new Context(server, "/", Context.SESSIONS);
        context.setResourceBase(baseUri.toASCIIString());
        context.addServlet(sh, "/*");

        //add web and restful context
        server.setHandlers(new Handler[]{wc,context});
        // start server
        server.start();
    }



  @Override
  protected void shutDown() throws Exception {
    server.stop();
  }

  private static void printUsage(Options opts) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("bin/hraven rest start", "", opts,
        "To run the REST server, execute bin/hraven rest start|stop [-p <port>]", true);
  }

  public static void main(String[] args) throws Exception {
    // parse commandline options
    Options opts = new Options();
    opts.addOption("p", "port", true, "Port for server to bind to (default 8080)");
    opts.addOption("a", "address", true, "IP address for server to bind to (default 0.0.0.0)");
    CommandLine cmd = null;
    try {
      cmd = new PosixParser().parse(opts, args);
    } catch (ParseException pe) {
      LOG.fatal("Failed to parse arguments", pe);
      printUsage(opts);
      System.exit(1);
    }

    String address = DEFAULT_ADDRESS;
    int port = DEFAULT_PORT;
    if (cmd.hasOption("p")) {
      try {
        port = Integer.parseInt(cmd.getOptionValue("p"));
      } catch (NumberFormatException nfe) {
        LOG.fatal("Invalid integer '"+cmd.getOptionValue("p")+"'", nfe);
        printUsage(opts);
        System.exit(2);
      }
    }
    if (cmd.hasOption("a")) {
      address = cmd.getOptionValue("a");
    }
    RestServer server = new RestServer(address, port);
    server.startUp();
    // run until we're done
  }
}
