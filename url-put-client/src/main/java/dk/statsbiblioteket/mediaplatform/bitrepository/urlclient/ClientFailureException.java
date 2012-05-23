package dk.statsbiblioteket.mediaplatform.bitrepository.urlclient;

import dk.statsbiblioteket.mediaplatform.bitrepository.urlclient.ClientExitCodes.ExitCodes;

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
