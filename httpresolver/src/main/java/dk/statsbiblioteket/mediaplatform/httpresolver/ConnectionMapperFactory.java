package dk.statsbiblioteket.mediaplatform.httpresolver;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ConnectionMapperFactory {

    private static Map<String, InputStream> connectionMapping;
    
    /** Private constructor for factory class. */
    private ConnectionMapperFactory() {}
    
    public synchronized static Map<String, InputStream> getConnectionMapping() {
        if(connectionMapping == null) {
            connectionMapping = Collections.synchronizedMap(new HashMap<String, InputStream>());
        }
        return connectionMapping;
    }
}
