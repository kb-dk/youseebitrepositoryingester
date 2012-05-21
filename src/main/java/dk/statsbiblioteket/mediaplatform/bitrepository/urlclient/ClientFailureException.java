package dk.statsbiblioteket.mediaplatform.bitrepository.urlclient;

/**
 * Method to indicate failure in the client operation. 
 * Contains the exit code indented to be returned upon exit of the java process. 
 */
public class ClientFailureException extends Exception {

    private final int exitCode;
    
    public ClientFailureException(String msg, int exitCode) {
        super(msg);
        this.exitCode = exitCode;
    }
    
    public int getExitCode() {
        return exitCode;
    }
}
