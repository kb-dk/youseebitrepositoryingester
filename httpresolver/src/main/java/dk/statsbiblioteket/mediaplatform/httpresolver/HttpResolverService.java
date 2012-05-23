package dk.statsbiblioteket.mediaplatform.httpresolver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;

@Path("/resolver")
public class HttpResolverService {

    private Map<String, InputStream> connectionMapping;

    public HttpResolverService() {
        connectionMapping = ConnectionMapperFactory.getConnectionMapping();
    }
    
    @GET
    @Path("/getfile/")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public StreamingOutput getFile() throws Exception {
        final InputStream is;
        synchronized(connectionMapping) {
            connectionMapping.put("foo", null);
            while(connectionMapping.get("foo") == null) {
                connectionMapping.wait();
            }
            is = connectionMapping.get("foo");
            connectionMapping.remove("foo");
        }
        
        return new StreamingOutput() {
            public void write(OutputStream output) throws IOException, WebApplicationException {
                try {
                    int i;
                    byte[] data = new byte[4096];
                    synchronized (is) {
                        while((i = is.read(data)) >= 0) {
                           output.write(data, 0, i);
                        }
                        is.close();
                        is.notifyAll();
                    }
                } catch (Exception e) {
                    throw new WebApplicationException(e);
                }
            }
        };
    }
    
    @PUT
    @Path("/uploadProxy/{ID}")
    public void uploadFile(@PathParam("ID") String id, InputStream is) throws InterruptedException {
        synchronized (connectionMapping) {
            if(connectionMapping.containsKey(id)) {
                connectionMapping.put(id, is);
                connectionMapping.notifyAll();
            } else {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }  
        }
        synchronized (is) {
            is.wait();
        }
    }
    
}
