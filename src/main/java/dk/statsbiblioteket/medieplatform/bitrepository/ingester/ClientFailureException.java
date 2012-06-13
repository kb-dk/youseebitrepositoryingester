package dk.statsbiblioteket.medieplatform.bitrepository.ingester;

import dk.statsbiblioteket.medieplatform.bitrepository.ingester.ClientExitCodes.ExitCodes;

/**
 * Method to indicate failure in the client operation. 
 * Contains the exit code indented to be returned upon exit of the java process. 
 */
public class ClientFailureException extends Exception {

    private final ExitCodes exitCode;
    
    public ClientFailureException(String msg, ExitCodes exitCode) {
        super(msg);
        this.exitCode = exitCode;
    }
    
    public ExitCodes getExitCode() {
        return exitCode;
    }
}
