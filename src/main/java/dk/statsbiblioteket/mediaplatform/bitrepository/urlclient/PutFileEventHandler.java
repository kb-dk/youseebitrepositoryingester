package dk.statsbiblioteket.mediaplatform.bitrepository.urlclient;

import org.bitrepository.client.eventhandler.EventHandler;
import org.bitrepository.client.eventhandler.OperationEvent;

/**
 *	Event handler for the asynchronous GetFileIDs method.   
 */
public class PutFileEventHandler implements EventHandler {

	private Object finishLock;
	private int finishStatusCode;
	private String finishMessage;
	
	public PutFileEventHandler() {
	    finishLock = new Object();	    
	}
	
	@SuppressWarnings("rawtypes")
    @Override
	public void handleEvent(OperationEvent event) {
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
		    finishStatusCode = ClientExitCodes.SUCCESS;
		    finishMessage = "Success";
		    finish();
		    break;
		case COMPONENT_FAILED:
		    break;
		case FAILED:
		    finishStatusCode = ClientExitCodes.CLIENT_PUT_ERROR;
		    finishMessage = "Client failed with: " + event.getInfo();
		    finish();
		    break;
		case NO_COMPONENT_FOUND:
		    break;
		case IDENTIFY_TIMEOUT: 
		    break;
		case WARNING:
		    break;
		}       
	}
	
	private void finish() {
	    finishLock.notifyAll();
	}
	
	public void waitForFinish() throws InterruptedException {
	    finishLock.wait();
	}
	
	public int getStatusCode() {
	    return finishStatusCode;
	}
	
	public String getFinishMessage() {
	    return finishMessage;
	}
	
}
