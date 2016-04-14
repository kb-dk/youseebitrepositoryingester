package dk.statsbiblioteket.medieplatform.bitrepository.ingester;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.net.MalformedURLException;
import java.net.URL;

import org.bitrepository.bitrepositoryelements.ChecksumDataForFileTYPE;
import org.bitrepository.bitrepositoryelements.ChecksumSpecTYPE;
import org.bitrepository.client.eventhandler.CompleteEvent;
import org.bitrepository.client.eventhandler.EventHandler;
import org.bitrepository.client.eventhandler.OperationFailedEvent;
import org.bitrepository.modify.putfile.PutFileClient;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FilePutterTest {

    protected static final String TEST_COLLECTION_ID = "bart";
    protected static final String ALLOWED_FILE_ID_PATTERN = "[a-zA-Z_\\.\\-0-9]{1,250}";
    protected static final String GOOD_TEST_FILEID 
        = "ksport_yousee.1426521600-2015-03-16-17.00.00_1426525200-2015-03-16-18.00.00_yousee.ts";
    protected static final String BAD_TEST_FILEID 
        = "ksport/yousee.1426521600-2015-03-16-17.00.00_1426525200-2015-03-16-18.00.00_yousee.ts";
    
    @Test
    public void successfulPutTest() throws MalformedURLException, ClientFailureException, InterruptedException {
        PutFileClient putClient = mock(PutFileClient.class);
        
        URL testFileLocation = new URL("file:///yousee-work/files/test");
        String testChecksum = "ab";
        
        FilePutter filePutter = new FilePutter(putClient, ALLOWED_FILE_ID_PATTERN, TEST_COLLECTION_ID);
        FilePutterRunner runner = new FilePutterRunner(filePutter, GOOD_TEST_FILEID, testFileLocation, testChecksum, 0L);
        Thread t = new Thread(runner);
        t.start();
                
        ArgumentCaptor<EventHandler> eventHandlerCaptor = ArgumentCaptor.forClass(EventHandler.class);
        verify(putClient, timeout(3000).times(1)).putFile(eq(TEST_COLLECTION_ID), eq(testFileLocation), 
                eq(GOOD_TEST_FILEID), eq(0L), any(ChecksumDataForFileTYPE.class), (ChecksumSpecTYPE) isNull(), 
                eventHandlerCaptor.capture(), any(String.class));
        
        CompleteEvent firstFileComplete = new CompleteEvent(TEST_COLLECTION_ID, null);
        firstFileComplete.setFileID(GOOD_TEST_FILEID);
        eventHandlerCaptor.getValue().handleEvent(firstFileComplete);
        
        t.join(3000);
        
        Assert.assertTrue(runner.finished);
        Assert.assertNull(runner.exception);
        
        verifyNoMoreInteractions(putClient);
    }
    
    @Test
    public void failingPutTest() throws MalformedURLException, InterruptedException {
        PutFileClient putClient = mock(PutFileClient.class);
        
        URL testFileLocation = new URL("file:///yousee-work/files/test");
        String testChecksum = "ab";
        
        FilePutter filePutter = new FilePutter(putClient, ALLOWED_FILE_ID_PATTERN, TEST_COLLECTION_ID);
        FilePutterRunner runner = new FilePutterRunner(filePutter, GOOD_TEST_FILEID, testFileLocation, testChecksum, 0L);
        Thread t = new Thread(runner);
        t.start();
                
        ArgumentCaptor<EventHandler> eventHandlerCaptor = ArgumentCaptor.forClass(EventHandler.class);
        verify(putClient, timeout(3000).times(1)).putFile(eq(TEST_COLLECTION_ID), eq(testFileLocation), 
                eq(GOOD_TEST_FILEID), eq(0L), any(ChecksumDataForFileTYPE.class), (ChecksumSpecTYPE) isNull(), 
                eventHandlerCaptor.capture(), any(String.class));
        
        OperationFailedEvent failureEvent = new OperationFailedEvent(TEST_COLLECTION_ID, null, null);
        failureEvent.setFileID(GOOD_TEST_FILEID);
        eventHandlerCaptor.getValue().handleEvent(failureEvent);
        
        t.join(3000);
        
        Assert.assertTrue(runner.finished);
        Assert.assertNotNull(runner.exception);
        Assert.assertEquals(runner.exception.getExitCode(), ClientExitCodes.ExitCodes.CLIENT_PUT_ERROR);        
        verifyNoMoreInteractions(putClient);
    }
    
    @Test
    public void badFileIDTest() throws MalformedURLException {
        PutFileClient putClient = mock(PutFileClient.class);
        
        URL testFileLocation = new URL("file:///yousee-work/files/test");
        String testChecksum = "ab";
        
        FilePutter filePutter = new FilePutter(putClient, ALLOWED_FILE_ID_PATTERN, TEST_COLLECTION_ID);
        try {
            filePutter.putFile(BAD_TEST_FILEID, testFileLocation, testChecksum, 0L);
        } catch (ClientFailureException e) {
            Assert.assertEquals(e.getExitCode(), ClientExitCodes.ExitCodes.ILLEGAL_FILEID);
        }
        verifyNoMoreInteractions(putClient);
    }
    
    /*
     * Work-around to get access via an ArgumentCaptor when dealing with asynchronious code. 
     */
    private class FilePutterRunner implements Runnable {
        boolean finished = false;
        ClientFailureException exception = null;
        
        FilePutter putter;
        String fileID;
        URL fileLocation;
        String checksum;
        long size;

        public FilePutterRunner(FilePutter putter, String fileID, URL location, String checksum, long size) {
            this.putter = putter;
            this.fileID = fileID;
            this.fileLocation = location;
            this.checksum = checksum;
            this.size = size;
        }
        
        public void run() {
            try {
                putter.putFile(fileID, fileLocation, checksum, size);
            } catch (ClientFailureException e) {
                exception = e;
            }
            finished = true;
        }
    }
}
