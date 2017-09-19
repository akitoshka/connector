package streaming;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.errors.ConnectExceptionMapper;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorPluginsResource;
import org.apache.kafka.connect.runtime.rest.resources.ConnectorsResource;
import org.apache.kafka.connect.runtime.rest.resources.RootResource;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.servlet.DispatcherType;
import java.util.EnumSet;


public class ProxyRestServer extends org.apache.kafka.connect.runtime.rest.RestServer {
    private final WorkerConfig config;
    private Server jettyServer;

    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 60 * 1000;

    public ProxyRestServer(WorkerConfig config) {
        super(config);
        this.config = config;

        // To make the advertised port available immediately, we need to do some configuration here
        String hostname = config.getString(WorkerConfig.REST_HOST_NAME_CONFIG);
        Integer port = config.getInt(WorkerConfig.REST_PORT_CONFIG);

        jettyServer = new Server();

        ServerConnector connector = new ServerConnector(jettyServer);
        if (hostname != null && !hostname.isEmpty())
            connector.setHost(hostname);
        connector.setPort(port);
        jettyServer.setConnectors(new Connector[]{connector});
    }

    @Override
    public void start(Herder herder) {

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(new JacksonJsonProvider());

        resourceConfig.register(RootResource.class);
        resourceConfig.register(new ConnectorsResource(herder));
        resourceConfig.register(new ConnectorPluginsResource(herder));

        resourceConfig.register(ConnectExceptionMapper.class);

        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletHolder servletHolder = new ServletHolder(servletContainer);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.addServlet(servletHolder, "/*");

        FilterHolder filterHolder = new FilterHolder(new AuthFilter());
        filterHolder.setName("auth-filter");
        context.addFilter(filterHolder, "/*", EnumSet.of(DispatcherType.REQUEST));

        RequestLogHandler requestLogHandler = new RequestLogHandler();
        Slf4jRequestLog requestLog = new Slf4jRequestLog();
        requestLog.setLoggerName(RestServer.class.getCanonicalName());
        requestLog.setLogLatency(true);
        requestLogHandler.setRequestLog(requestLog);

        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[]{context, new DefaultHandler(), requestLogHandler});

        /* Needed for graceful shutdown as per `setStopTimeout` documentation */
        StatisticsHandler statsHandler = new StatisticsHandler();
        statsHandler.setHandler(handlers);
        jettyServer.setHandler(statsHandler);
        jettyServer.setStopTimeout(GRACEFUL_SHUTDOWN_TIMEOUT_MS);
        jettyServer.setStopAtShutdown(true);

        try {
            jettyServer.start();
        } catch (Exception e) {
            throw new ConnectException("Unable to start REST server", e);
        }

    }


}