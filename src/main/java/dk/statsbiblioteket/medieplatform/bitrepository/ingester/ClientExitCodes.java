package dk.statsbiblioteket.medieplatform.bitrepository.ingester;

/**
 * Class to hold the different possible exit codes allowed in the process 
 */
public class ClientExitCodes {

    public enum ExitCodes {
        SUCCESS (0),
        INPUT_PARAM_COUNT_ERROR (1),
        LOGGING_ERROR (2),
        CONFIG_DIR_ERROR (3),
        FILE_SIZE_ERROR (4),
        CHECKSUM_ERROR (5),
        JSON_ERROR (6),
        URL_ERROR (7),
        ILLEGAL_FILEID (8),
        CLIENT_PUT_ERROR (9),
        UNEXPECTED_ERROR(100);
        
        private final int value;
        ExitCodes(int value) { this.value = value; }
        
        public int getCode() { return value; }
    }
    
}
