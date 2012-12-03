package dk.statsbiblioteket.medieplatform.bitrepository.ingester;

import dk.statsbiblioteket.medieplatform.bitrepository.ingester.ClientExitCodes.ExitCodes;
import org.bitrepository.bitrepositoryelements.ChecksumDataForFileTYPE;
import org.bitrepository.bitrepositoryelements.ChecksumSpecTYPE;
import org.bitrepository.bitrepositoryelements.ChecksumType;
import org.bitrepository.common.settings.Settings;
import org.bitrepository.common.settings.SettingsProvider;
import org.bitrepository.common.settings.XMLFileSettingsLoader;
import org.bitrepository.common.utils.Base16Utils;
import org.bitrepository.common.utils.CalendarUtils;
import org.bitrepository.modify.ModifyComponentFactory;
import org.bitrepository.modify.putfile.PutFileClient;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import javax.jms.JMSException;

/**
 * Class handling putting of a file this includes
 * - Loading of settings from configuration directory
 * - Validation of FileID
 * - Connection to the Bitrepository 
 * - Calls to the Bitrepository reference client and handling of events.   
 */
public class FilePutter {

    private final static String CLIENT_ID_PROPERTY 
        = "dk.statsbiblioteket.medieplatform.bitrepository.clientid";
    private final static String CLIENT_CERTIFICATE_PROPERTY 
        = "dk.statsbiblioteket.medieplatform.bitrepository.clientcertificatefile";
    private final static String BASE_URL_PROPERTY 
        = "dk.statsbiblioteket.medieplatform.bitrepository.baseurl";
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final String fileID;
    private final URL fileURL;
    private final String checksum;
    private final long fileSize;
    private final Settings settings;
    private final Properties properties;
    
    private final PutFileClient putFileClient;
    
    /**
     * Constructor. Validates inputs as part of the construction process. 
     * @throws ClientFailureException in case of illegal inputs. 
     */
    public FilePutter(String configDir, Properties properties, String fileID, String url, String checksum, 
            long fileSize) throws ClientFailureException {
        SettingsProvider settingsLoader = new SettingsProvider(new XMLFileSettingsLoader(configDir),
                properties.getProperty(CLIENT_ID_PROPERTY));
        settings = settingsLoader.getSettings();
        this.properties = properties;
        verifyProperties();
        this.fileID = fileID;
        if(!fileID.matches(settings.getCollectionSettings().getProtocolSettings().getAllowedFileIDPattern())) {
            throw new ClientFailureException("The fileID is not allowed. FileID must match: " + 
                    settings.getCollectionSettings().getProtocolSettings().getAllowedFileIDPattern(), 
                    ExitCodes.ILLEGAL_FILEID);
        }
        
        try {
            fileURL = new URL(url);
        } catch (MalformedURLException e) {
            throw new ClientFailureException("Malformed URL for filelocation: " + url, ExitCodes.URL_ERROR);
        }
        
        this.checksum = checksum;
        this.fileSize = fileSize;
        
        PermissionStore permissionStore = new PermissionStore();
        MessageAuthenticator authenticator = new BasicMessageAuthenticator(permissionStore);
        MessageSigner signer = new BasicMessageSigner();
        OperationAuthorizor authorizer = new BasicOperationAuthorizor(permissionStore);
        SecurityManager securityManager = new BasicSecurityManager(settings.getCollectionSettings(), 
                configDir + "/" + properties.getProperty(CLIENT_CERTIFICATE_PROPERTY),
                authenticator, signer, authorizer, permissionStore, properties.getProperty(CLIENT_ID_PROPERTY));
        putFileClient = ModifyComponentFactory.getInstance().retrievePutClient(settings, securityManager, 
                properties.getProperty(CLIENT_ID_PROPERTY));
    }
    
    /**
     * Blocking call to the put file client. 
     * @throws ClientFailureException in case of failure 
     */
    public void putFile() throws ClientFailureException {
        PutFileEventHandler handler = new PutFileEventHandler();
        ChecksumDataForFileTYPE checksumData = new ChecksumDataForFileTYPE();
        checksumData.setChecksumValue(Base16Utils.encodeBase16(checksum));
        checksumData.setCalculationTimestamp(CalendarUtils.getNow());
        ChecksumSpecTYPE checksumSpec = new ChecksumSpecTYPE();
        checksumSpec.setChecksumType(ChecksumType.MD5);
        checksumData.setChecksumSpec(checksumSpec);

        
        putFileClient.putFile(fileURL, fileID, fileSize, checksumData, null, handler, "Initial ingest of file");
        
        try {
            handler.waitForFinish();
            log.trace("Done waiting for the client to finish");
            if(handler.getStatusCode() != ExitCodes.SUCCESS) {
                throw new ClientFailureException(handler.getFinishMessage(), handler.getStatusCode());
            }
        } catch (InterruptedException e) {
            throw new ClientFailureException("Client was interrupted", ExitCodes.CLIENT_PUT_ERROR); 
        }
    }
    
    /**
     * Method to shutdown the client properly  
     */
    public void shutdown() {
    	try {
			MessageBusManager.getMessageBus(settings.getCollectionID()).close();
		} catch (JMSException e) {
			log.warn("Failed to shutdown messagebus connection", e);
		}
    }    
    
    /**
     * Get the url to be returned to the workflow.  
     */
    public String getUrl() {
        return properties.getProperty(BASE_URL_PROPERTY) + fileID;
    }
    
    /**
     * Verify required properties 
     */
    private void verifyProperties() {
        if(properties.getProperty(BASE_URL_PROPERTY) == null) {
            throw new RuntimeException(BASE_URL_PROPERTY + " is null");
        }
        if(properties.getProperty(CLIENT_CERTIFICATE_PROPERTY) == null) {
            throw new RuntimeException(CLIENT_CERTIFICATE_PROPERTY + " is null");
        }
        if(properties.getProperty(CLIENT_ID_PROPERTY) == null) {
            throw new RuntimeException(CLIENT_ID_PROPERTY + " is null");
        }
    }
}
