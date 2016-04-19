package dk.statsbiblioteket.medieplatform.bitrepository.ingester;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
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
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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
        
        PutFileEventHandler eventHandlerForTest = new PutFileEventHandler();
        /* Setup putClient mock to stimulate eventhandler upon calling put */
        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) {
                CompleteEvent firstFileComplete = new CompleteEvent(TEST_COLLECTION_ID, null);
                firstFileComplete.setFileID(GOOD_TEST_FILEID);
                eventHandlerForTest.handleEvent(firstFileComplete);
                return null;
                
            }
        }).when(putClient).putFile(any(String.class), any(URL.class), any(String.class), any(Long.class), 
                any(ChecksumDataForFileTYPE.class), any(ChecksumSpecTYPE.class), 
                any(EventHandler.class), any(String.class));
        
        FilePutter filePutter = Mockito.spy(new FilePutter(putClient, ALLOWED_FILE_ID_PATTERN, TEST_COLLECTION_ID));
        
        doReturn(eventHandlerForTest).when(filePutter).getEventHandler();
        
        filePutter.putFile(GOOD_TEST_FILEID, testFileLocation, testChecksum, 0L);
        
        verify(putClient, timeout(3000).times(1)).putFile(eq(TEST_COLLECTION_ID), eq(testFileLocation), 
                eq(GOOD_TEST_FILEID), eq(0L), any(ChecksumDataForFileTYPE.class), (ChecksumSpecTYPE) isNull(), 
                eq(eventHandlerForTest), any(String.class));
        
        verifyNoMoreInteractions(putClient);
    }
    
    @Test
    public void failingPutTest() throws MalformedURLException, InterruptedException {
        PutFileClient putClient = mock(PutFileClient.class);
        
        URL testFileLocation = new URL("file:///yousee-work/files/test");
        String testChecksum = "ab";
        
        PutFileEventHandler eventHandlerForTest = new PutFileEventHandler();
        /* Setup putClient mock to stimulate eventhandler upon calling put */
        doAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) {
                OperationFailedEvent failureEvent = new OperationFailedEvent(TEST_COLLECTION_ID, null, null);
                failureEvent.setFileID(GOOD_TEST_FILEID);
                eventHandlerForTest.handleEvent(failureEvent);
                return null;
                
            }
        }).when(putClient).putFile(any(String.class), any(URL.class), any(String.class), any(Long.class), 
                any(ChecksumDataForFileTYPE.class), any(ChecksumSpecTYPE.class), 
                any(EventHandler.class), any(String.class));
        
        FilePutter filePutter = Mockito.spy(new FilePutter(putClient, ALLOWED_FILE_ID_PATTERN, TEST_COLLECTION_ID));
        
        doReturn(eventHandlerForTest).when(filePutter).getEventHandler();
        
        try {
            filePutter.putFile(GOOD_TEST_FILEID, testFileLocation, testChecksum, 0L);
            Assert.fail("Expected a ClientFailureException");
        } catch (ClientFailureException e) {
            Assert.assertEquals(e.getExitCode(), ClientExitCodes.ExitCodes.CLIENT_PUT_ERROR);
        }
        
        verify(putClient, timeout(3000).times(1)).putFile(eq(TEST_COLLECTION_ID), eq(testFileLocation), 
                eq(GOOD_TEST_FILEID), eq(0L), any(ChecksumDataForFileTYPE.class), (ChecksumSpecTYPE) isNull(), 
                eq(eventHandlerForTest), any(String.class));
        
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
            Assert.fail("Expected a ClientFailureException");
        } catch (ClientFailureException e) {
            Assert.assertEquals(e.getExitCode(), ClientExitCodes.ExitCodes.ILLEGAL_FILEID);
        }
        verifyNoMoreInteractions(putClient);
    }

}
