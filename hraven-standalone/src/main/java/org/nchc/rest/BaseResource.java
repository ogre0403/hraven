package org.nchc.rest;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by 1403035 on 2015/2/2.
 */

@Path("/")
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

    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/")
    public String template() throws IOException {
        InputStream is = RestJSONResource.class.getClass().getResourceAsStream("/index_template");
        String template ="";
        try {
            /**TODO:
             * get user name by IAM, then replace marker in index_template
             * */
            template = IOUtils.toString(is);
        }catch (IOException ioe){
            LOG.error(ioe.toString());
        }finally {
            is.close();
        }
        return template;
    }
}
