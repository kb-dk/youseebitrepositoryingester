package dk.statsbiblioteket.medieplatform.bitrepository.ingester;

import org.bitrepository.client.eventhandler.EventHandler;
import org.bitrepository.client.eventhandler.OperationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.statsbiblioteket.medieplatform.bitrepository.ingester.ClientExitCodes.ExitCodes;

/**
 *	Event handler for the asynchronous GetFileIDs method.   
 */
public class PutFileEventHandler implements EventHandler {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private Object finishLock;
    private ExitCodes finishStatusCode;
    private String finishMessage;

    public PutFileEventHandler() {
        finishLock = new Object();	    
    }

    @Override
    public void handleEvent(OperationEvent event) {
        log.debug("Got event: " + event.toString());
        switch(event.getType()) {
        case IDENTIFY_REQUEST_SENT:
            break;
        case COMPONENT_IDENTIFIED:
            break;
        case IDENTIFICATION_COMPLETE:
            break;
        case REQUEST_SENT:
            break;
        case PROGRESS:
            break;
        case COMPONENT_COMPLETE:
            break;
        case COMPLETE:
            finishStatusCode = ExitCodes.SUCCESS;
            finishMessage = "Success";
            finish();
            break;
        case COMPONENT_FAILED:
        	log.warn(event.toString());
            break;
        case FAILED:
        	log.error(event.toString());
        	finishStatusCode = ExitCodes.CLIENT_PUT_ERROR;
            finishMessage = "Client failed with: " + event.getInfo();
            finish();
            break;
        case NO_COMPONENT_FOUND:
            break;
        case IDENTIFY_TIMEOUT: 
            break;
        case WARNING:
        	log.warn(event.toString());
            break;
        }       
    }

    private void finish() {
    	log.trace("Finish method invoked");
        synchronized (finishLock) {
        	log.trace("Finish method entered synchronized block");
            finishLock.notifyAll();
            log.trace("Finish method notified All");            
        }
    }

    public void waitForFinish() throws InterruptedException {
        synchronized (finishLock) {
        	log.trace("Thread waiting for put client to finish");
            finishLock.wait();            
            log.trace("Put client have indicated it's finished.");
        }
    }

    public ExitCodes getStatusCode() {
        return finishStatusCode;
    }

    public String getFinishMessage() {
        return finishMessage;
    }

}
