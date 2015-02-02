package org.nchc.rest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

/**
 * Created by 1403035 on 2015/2/2.
 */
public class BaseResource {
    private static final Log LOG = LogFactory.getLog(BaseResource.class);
    protected static final String SLASH = "/" ;

    protected static final Configuration HBASE_CONF = HBaseConfiguration.create();

    protected static final ThreadLocal<QueryJobService> queryThreadLocal =
            new ThreadLocal<QueryJobService>() {
                @Override
                protected QueryJobService initialValue() {
                    try {
                        LOG.info("Initializing queryJobService");
                        return new QueryJobService(HBASE_CONF);
                    } catch (IOException e) {
                        throw new RuntimeException("Could not initialize queryJobService", e);
                    }
                }
            };

    protected static QueryJobService getQueryService(){
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Returning QueryJobService %s bound to thread %s",
                    queryThreadLocal.get(), Thread.currentThread().getName()));
        }
        return queryThreadLocal.get();
    }
}
