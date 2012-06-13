package dk.statsbiblioteket.medieplatform.bitrepository.ingester;

/**
 * Class to hold the different possible exit codes allowed in the process 
 */
public class ClientExitCodes {

    public enum ExitCodes {
        SUCCESS (0),
        INPUT_PARAM_COUNT_ERROR (1),
        CONFIG_DIR_ERROR (2),
        FILE_SIZE_ERROR (3),
        CHECKSUM_ERROR (4),
        JSON_ERROR (5),
        URL_ERROR (6),
        ILLEGAL_FILEID (7),
        CLIENT_PUT_ERROR (8);
        
        private final int value;
        ExitCodes(int value) { this.value = value; }
        
        public int getCode() { return value; }
    }
    
}
