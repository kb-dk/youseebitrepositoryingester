package dk.statsbiblioteket.medieplatform.bitrepository.ingester;

import dk.statsbiblioteket.medieplatform.bitrepository.ingester.ClientExitCodes.ExitCodes;

import org.bitrepository.common.settings.Settings;
import org.bitrepository.common.settings.SettingsProvider;
import org.bitrepository.common.settings.XMLFileSettingsLoader;
import org.bitrepository.modify.ModifyComponentFactory;
import org.bitrepository.modify.putfile.PutFileClient;
import org.bitrepository.protocol.messagebus.MessageBus;
import org.bitrepository.protocol.messagebus.MessageBusManager;
import org.bitrepository.protocol.security.BasicMessageAuthenticator;
import org.bitrepository.protocol.security.BasicMessageSigner;
import org.bitrepository.protocol.security.BasicOperationAuthorizor;
import org.bitrepository.protocol.security.BasicSecurityManager;
import org.bitrepository.protocol.security.MessageAuthenticator;
import org.bitrepository.protocol.security.MessageSigner;
import org.bitrepository.protocol.security.OperationAuthorizor;
import org.bitrepository.protocol.security.PermissionStore;
import org.bitrepository.protocol.security.SecurityManager;
import org.bitrepository.protocol.utils.LogbackConfigLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import javax.jms.JMSException;

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
    
    private final static String CLIENT_ID_PROPERTY 
        = "dk.statsbiblioteket.medieplatform.bitrepository.clientid";
    private final static String CLIENT_CERTIFICATE_PROPERTY 
        = "dk.statsbiblioteket.medieplatform.bitrepository.clientcertificatefile";
    private final static String BASE_URL_PROPERTY 
        = "dk.statsbiblioteket.medieplatform.bitrepository.baseurl";
    private final static String COLLECTION_ID_PROPERTY 
        = "dk.statsbiblioteket.medieplatform.bitrepository.collectionid";

    private final static Logger log = LoggerFactory.getLogger(Ingester.class);
    
    private String confDir;
    private URL fileLocation;
    private String fileID;
    private String checksum;
    private Long filesize;
    private Properties properties;
    
    public static void main(String[] args) {
        try {
            Ingester ingester = new Ingester(args);
            String url = ingester.ingest();
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
        log.info("Initializing ingester with args: " + args);
        loadAndVerifyInputParams(args);
        setupLogging(confDir);
        loadAndVerifyProperties(confDir);
    }
    
    /**
     * Method to setup and handle the put. 
     * @return url The url of the newly put file in the repository
     * @throws ClientFailureException In case of failure putting the file. 
     */
    public String ingest() throws ClientFailureException {
        String clientID = properties.getProperty(CLIENT_ID_PROPERTY);
        String certificateLocation = confDir + "/" + properties.getProperty(CLIENT_CERTIFICATE_PROPERTY);
        String baseUrl = properties.getProperty(BASE_URL_PROPERTY);
        String collectionID = properties.getProperty(COLLECTION_ID_PROPERTY);
        
        log.debug("Loading bitrepository settings from dir: '{}'", confDir);
        SettingsProvider settingsLoader = new SettingsProvider(new XMLFileSettingsLoader(confDir), clientID);
        Settings settings = settingsLoader.getSettings();
        log.debug("Creating bitrepository client");
        PutFileClient putFileClient = createPutClient(clientID, certificateLocation, settings);
        
        String allowedFileIDPattern 
            = settings.getRepositorySettings().getProtocolSettings().getAllowedFileIDPattern();
        
        log.debug("Starting put of file: '{}'", fileID);
        try {
            FilePutter putter = new FilePutter(putFileClient, allowedFileIDPattern, collectionID);  
            putter.putFile(fileID, fileLocation, checksum, filesize);
            return baseUrl + fileID; 
        } finally {
            shutdown();
        }
    }
    
    /**
     * Helper method to obtain the {@link PutFileClient}.
     * @param clientID The ID of the client
     * @param certificateLocation The location of the clients certificate in the local filesystem
     * @param settings The {@link Settings} for the Bitrepository.org client
     * @return {@link PutFileClient} The PutFileClient 
     */
    private PutFileClient createPutClient(String clientID, String certificateLocation, Settings settings) {
        PermissionStore permissionStore = new PermissionStore();
        MessageAuthenticator authenticator = new BasicMessageAuthenticator(permissionStore);
        MessageSigner signer = new BasicMessageSigner();
        OperationAuthorizor authorizer = new BasicOperationAuthorizor(permissionStore);
        SecurityManager securityManager = new BasicSecurityManager(settings.getRepositorySettings(), 
                certificateLocation, authenticator, signer, authorizer, permissionStore, clientID);
        return ModifyComponentFactory.getInstance().retrievePutClient(settings, securityManager, clientID);
    }
    
    /**
     * Method to shutdown the messagebus connection. 
     */
    private void shutdown() {
        try {
            MessageBus messageBus = MessageBusManager.getMessageBus();
            if (messageBus != null) {
                MessageBusManager.getMessageBus().close();
            }
        } catch (JMSException e) {
            log.warn("Failed to shutdown messagebus connection", e);
        }
    }
    
    /**
     * Method to write the output of program upon success. 
     * @param url The URL for the file in the repository
     * @throws ClientFailureException in case of failure to serialize the result
     */
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
     * Method to verify the input parameters and set the member variables. 
     * Verifies the validity of:
     * - Number of arguments 
     * - The existence and readability of the configuration directory
     * - The ability to parse the file size parameter as a long
     * - The length and content of the checksum parameter  
     * - If the file location, is a proper URL
     * If validation fails error is printed to console and program is exited. 
     * @param args The arguments to parse
     * @throws ClientFailureException in case there is an issue with a parameter
     */
    private void loadAndVerifyInputParams(String[] args) throws ClientFailureException {
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
        

        try {
            fileLocation = new URL(args[FILE_LOCATION_ARG_INDEX]);
        } catch (MalformedURLException e) {
            throw new ClientFailureException("Malformed URL for fileLocation: " + fileLocation, ExitCodes.URL_ERROR);
        }
        
        fileID = args[FILEID_ARG_INDEX];
    }
    
    /**
     * Method to handle setup of logging 
     * @param configDir String representation of the directory to find 'logback.xml'
     * @throws ClientFailureException in case setup of logging fails 
     */
    private static void setupLogging(String configDir) throws ClientFailureException {
        try {
            new LogbackConfigLoader(configDir + "/logback.xml");
        } catch (Exception e) {
            throw new ClientFailureException("Logging setup failed!", ExitCodes.LOGGING_ERROR);
        } 
    }
    
    /**
     * Load and verify properties from configuration file
     * @param configDir The configuration dir to load the properties from 
     */
    private void loadAndVerifyProperties(String configDir) {
        properties = new Properties();
        try {
            String propertiesFile = configDir + "/" + INGESTER_PROPERTIES_FILE;
            BufferedReader reader = new BufferedReader(new FileReader(propertiesFile));
            properties.load(reader);
                    
            if(properties.getProperty(BASE_URL_PROPERTY) == null) {
                throw new RuntimeException(BASE_URL_PROPERTY + " is null");
            }
            if(properties.getProperty(CLIENT_CERTIFICATE_PROPERTY) == null) {
                throw new RuntimeException(CLIENT_CERTIFICATE_PROPERTY + " is null");
            }
            if(properties.getProperty(CLIENT_ID_PROPERTY) == null) {
                throw new RuntimeException(CLIENT_ID_PROPERTY + " is null");
            }
            if(properties.getProperty(COLLECTION_ID_PROPERTY) == null) {
                throw new RuntimeException(COLLECTION_ID_PROPERTY + " is null");
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
    
}
