package dk.statsbiblioteket.medieplatform.bitrepository.ingester;

import java.net.URL;

import org.bitrepository.bitrepositoryelements.ChecksumDataForFileTYPE;
import org.bitrepository.bitrepositoryelements.ChecksumSpecTYPE;
import org.bitrepository.bitrepositoryelements.ChecksumType;
import org.bitrepository.common.settings.Settings;
import org.bitrepository.common.utils.Base16Utils;
import org.bitrepository.common.utils.CalendarUtils;
import org.bitrepository.modify.putfile.PutFileClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.statsbiblioteket.medieplatform.bitrepository.ingester.ClientExitCodes.ExitCodes;

/**
 * Class handling putting of a file this includes
 * - Validation of FileID
 * - Calls to the Bitrepository reference client and handling of events.   
 */
public class FilePutter {
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final String collectionID;
    private final Settings settings;
    private final PutFileClient putFileClient;
    
    /**
     * Constructor.  
     * @param putClient The {@link PutFileClient} to use for putting files
     * @param settings The {@link Settings} for the bitrepository
     * @param collectionID The ID of the collection to put files into. 
     */
    public FilePutter(PutFileClient putClient, Settings settings, String collectionID) {
        this.settings = settings;
        this.collectionID = collectionID;
        putFileClient = putClient;
    }
    
    /**
     * Blocking call to the put file client. 
     * @param fileID The ID of the file in the repository
     * @param fileLocation The URL of the location where the file to be put can be found
     * @param checksum The checksum of the file to be put
     * @param fileSize The size of the file to be put
     * @throws ClientFailureException in case of failure 
     */
    public void putFile(String fileID, URL fileLocation, String checksum, long fileSize) throws ClientFailureException {
        PutFileEventHandler handler = new PutFileEventHandler();
        ChecksumDataForFileTYPE checksumData = createChecksumData(checksum);
        
        if(!fileID.matches(settings.getRepositorySettings().getProtocolSettings().getAllowedFileIDPattern())) {
            throw new ClientFailureException("The fileID is not allowed. FileID must match: " + 
                    settings.getRepositorySettings().getProtocolSettings().getAllowedFileIDPattern(), 
                    ExitCodes.ILLEGAL_FILEID);
        }
        
        putFileClient.putFile(collectionID, fileLocation, fileID, fileSize, checksumData, null, handler, 
                "Initial ingest of file");
        
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
     * Method to build the Bitrepository.org {@link ChecksumDataForFileTYPE} datastructure
     * @param checksum The checksum to put into the datastructure
     * @return {@link ChecksumDataForFileTYPE} datastructure that represents the checksum 
     */
    protected ChecksumDataForFileTYPE createChecksumData(String checksum) {
        ChecksumDataForFileTYPE checksumData = new ChecksumDataForFileTYPE();
        checksumData.setChecksumValue(Base16Utils.encodeBase16(checksum));
        checksumData.setCalculationTimestamp(CalendarUtils.getNow());
        ChecksumSpecTYPE checksumSpec = new ChecksumSpecTYPE();
        checksumSpec.setChecksumType(ChecksumType.MD5);
        checksumData.setChecksumSpec(checksumSpec);
        return checksumData;
    }

}
