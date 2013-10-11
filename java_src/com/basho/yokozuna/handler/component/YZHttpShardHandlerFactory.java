package com.basho.yokozuna.handler.component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.HttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandler;

// The point of this class is to allow Yokozuna to replace
// HttpShardHandler.class with its own implementation.
public class YZHttpShardHandlerFactory extends HttpShardHandlerFactory {

    @Override
    public void init(PluginInfo info) {
        super(info);
        super.defaultClient.getParams().setParameter(HttpConnectionParams.STALE_CONNECTION_CHECK, false);
        super.defaultClient.getParams().setParameter(HttpConnectionParams.TCP_NODELAY, true);

        final org.apache.http.conn.ClientConnectionManager mgr = super.httpClient.getConnectionManager();

        // NOTE: The sweeper task is assuming hard-coded Jetty max-idle of 50s.
        final Runnable sweeper = new Runnable() {
                public void run() {
                    mgr.closeIdleConnections(40, TimeUnit.SECONDS);
                }
            };
        final ScheduledExecutorService stp = Executors.newScheduledThreadPool(1);
        stp.scheduleWithFixedDelay(sweeper, 5, 5, TimeUnit.SECONDS);
    }

    @Override
    public ShardHandler getShardHandler(final HttpClient httpClient) {
        return new YZHttpShardHandler(this, httpClient);
    }
}
