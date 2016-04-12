package dk.statsbiblioteket.medieplatform.bitrepository.ingester;

import dk.statsbiblioteket.medieplatform.bitrepository.ingester.ClientExitCodes.ExitCodes;

import org.bitrepository.protocol.utils.LogbackConfigLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * The main executable class for ingesting files in a configured bit repository.
 */
public class Ingester {
    public static final int CONFIG_DIR_ARG_INDEX = 0;
    public static final int FILE_LOCATION_ARG_INDEX = 1;
    public static final int FILEID_ARG_INDEX = 2;
    public static final int CHECKSUM_ARG_INDEX = 3;
    public static final int FILESIZE_ARG_INDEX = 4;
    public static final String INGESTER_PROPERTIES_FILE = "ingester.properties";

    private final static Logger log = LoggerFactory.getLogger(Ingester.class);
    
    private String confDir;
    private String fileLocation;
    private String fileID;
    private String checksum;
    private Long filesize;
    private Properties properties;
    
    public static void main(String[] args) {
        try {
            Ingester ingester = new Ingester(args);
            String url = ingester.ingest(args);
            writeSuccessOutput(url);
            System.exit(ExitCodes.SUCCESS.getCode());
        } catch (ClientFailureException e) {
            log.error("Failure running ingest", e);
            System.out.println(e.getMessage());
            System.exit(e.getExitCode().getCode());
        } catch (Exception e) {
            log.error("Caught unexpected exception", e);
            System.exit(ExitCodes.UNEXPECTED_ERROR.getCode());
        }
    }
    
    public Ingester(String[] args) throws ClientFailureException {
        verifyAndLoadInputParams(args);
    }
    
    public String ingest(String[] args) throws ClientFailureException {
        String url = null;
        FilePutter putter = null;
        
        try {
            setupLogging(confDir);
            properties = loadProperties(confDir);
            log.info("Ingest of file requested: " + args);
            log.debug("Starting client");
            putter = new FilePutter(confDir, properties, fileID, fileLocation, checksum, filesize);  
            putter.putFile();
            url = putter.getUrl();
        } finally {
            if(putter != null) {
                log.debug("Shutting down messagebus connection");
                putter.shutdown();
            }
        }
        return url;
    }
    
    private static void writeSuccessOutput(String url) throws ClientFailureException {
        JSONObject obj = new JSONObject();
        try {
            obj.put("UrlToFile", url);
            System.out.println(obj.toString());
        } catch (JSONException e) {
            throw new ClientFailureException("Failed to generate JSON output", ExitCodes.JSON_ERROR);
        }
    }
    
    /**
     * Method to verify the input parameters
     * Verifies the validity of:
     * - Number of arguments 
     * - The existence and readability of the configuration directory
     * - The ability to parse the file size parameter as a long
     * - The length and content of the checksum parameter  
     * If validation fails error is printed to console and program is exited. 
     */
    private void verifyAndLoadInputParams(String[] args) throws ClientFailureException {
        if(args.length != 5) {
            throw new ClientFailureException("Unexpected number of arguments, got " + args.length + " but expected 5" + 
                    "Expecting: ConfigDirPath FileUrl FileID FileChecksum FileSize", 
                    ExitCodes.INPUT_PARAM_COUNT_ERROR);
        }
        
        File configDir = new File(args[CONFIG_DIR_ARG_INDEX]);
        if(!configDir.exists()) {
            throw new ClientFailureException("Config dir (parm " + CONFIG_DIR_ARG_INDEX + ": " +
                    configDir.getAbsolutePath() + ") doesn't exist!",
                    ExitCodes.CONFIG_DIR_ERROR);
        }
        if(!configDir.isDirectory()) {
            throw new ClientFailureException("Config dir (parm " + CONFIG_DIR_ARG_INDEX + ": " + configDir +
                    ") is not a directory!",
                    ExitCodes.CONFIG_DIR_ERROR);
        }
        if(!configDir.canRead()) {
            throw new ClientFailureException("Config dir '" + configDir + "' cannot be read!",
                    ExitCodes.CONFIG_DIR_ERROR);
        }
        confDir = args[CONFIG_DIR_ARG_INDEX];
        
        try {
            filesize = Long.parseLong(args[FILESIZE_ARG_INDEX]);
        } catch (Exception e) {
            throw new ClientFailureException("Failed to parse filesize argument " + args[FILESIZE_ARG_INDEX] +
                    " as long.", ExitCodes.FILE_SIZE_ERROR);
        }
                
        checksum = args[CHECKSUM_ARG_INDEX];
        if((checksum.length() % 2) != 0) {
            throw new ClientFailureException("Checksum argument " + checksum +
                    " does not contain an even number of characters.",
                    ExitCodes.CHECKSUM_ERROR);
        }
        if(!checksum.matches("^\\p{XDigit}*$")) {
            throw new ClientFailureException("Checksum argument " + checksum +
                    " contains non hexadecimal value!", ExitCodes.CHECKSUM_ERROR);
        } 
        
        fileLocation = args[FILE_LOCATION_ARG_INDEX];
        fileID = args[FILEID_ARG_INDEX];
    }
    
    private static void setupLogging(String configDir) throws ClientFailureException {
        try {
            new LogbackConfigLoader(configDir + "/logback.xml");
        } catch (Exception e) {
            throw new ClientFailureException("Logging setup failed!", ExitCodes.LOGGING_ERROR);
        } 
    }
    
    /**
     * Load properties from configuration file 
     */
    private static Properties loadProperties(String configDir) {
        Properties properties = new Properties();
        try {
            String propertiesFile = configDir + "/" + INGESTER_PROPERTIES_FILE;
            BufferedReader reader = new BufferedReader(new FileReader(propertiesFile));
            properties.load(reader);
            return properties;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
