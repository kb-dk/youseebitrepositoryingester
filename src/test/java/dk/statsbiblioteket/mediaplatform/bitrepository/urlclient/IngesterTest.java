package dk.statsbiblioteket.mediaplatform.bitrepository.urlclient;

import org.bitrepository.bitrepositorymessages.IdentifyPillarsForPutFileRequest;
import org.bitrepository.bitrepositorymessages.IdentifyPillarsForPutFileResponse;
import org.bitrepository.bitrepositorymessages.PutFileFinalResponse;
import org.bitrepository.bitrepositorymessages.PutFileRequest;
import org.bitrepository.client.DefaultFixtureClientTest;
import org.bitrepository.modify.putfile.TestPutFileMessageFactory;
import org.bitrepository.protocol.utils.LogbackConfigLoader;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class IngesterTest extends DefaultFixtureClientTest {
    public static final String CONFIG_DIR_ARG = "target/testclasses/config";
    public static final String FILEID_ARG = DEFAULT_FILE_ID;
    public static final String FILE_LOCATION_ARG = "file://src/test/resources/test-files/" + DEFAULT_FILE_ID;
    public static final String CHECKSUM_ARG = "AA";
    public static final String FILESIZE_ARG = "7";

    protected TestPutFileMessageFactory messageFactory;

    @BeforeMethod(alwaysRun=true)
    public void initialise() throws Exception {
        new LogbackConfigLoader(CONFIG_DIR_ARG + "/logback.xml");
        messageFactory = new TestPutFileMessageFactory(settings.getCollectionID());
    }

    @Test
    public void testClient() throws Exception {
        addDescription("Tests whether a file can be ingested through the client on a single pillar");
        settings.getCollectionSettings().getClientSettings().getPillarIDs().clear();
        settings.getCollectionSettings().getClientSettings().getPillarIDs().add(PILLAR1_ID);

        addStep("Request the ingest of a file.",
                "A IdentifyPillarsForPutFileRequest should be sent to the pillar.");
        String[] args = new String[5];
        args[Ingester.CONFIG_DIR_ARG_INDEX]= CONFIG_DIR_ARG;
        args[Ingester.FILEID_ARG_INDEX]= FILEID_ARG;
        args[Ingester.FILE_LOCATION_ARG_INDEX] = FILE_LOCATION_ARG;
        args[Ingester.CHECKSUM_ARG_INDEX]= CHECKSUM_ARG;
        args[Ingester.FILESIZE_ARG_INDEX] = FILESIZE_ARG;
        Ingester.main(args);

        IdentifyPillarsForPutFileRequest receivedIdentifyRequestMessage = collectionDestination.waitForMessage(
                IdentifyPillarsForPutFileRequest.class);
        Assert.assertEquals(receivedIdentifyRequestMessage,
                messageFactory.createIdentifyPillarsForPutFileRequest(
                        receivedIdentifyRequestMessage.getCorrelationID(),
                        receivedIdentifyRequestMessage.getReplyTo(),
                        receivedIdentifyRequestMessage.getTo(),
                        args[Ingester.FILEID_ARG_INDEX],
                        Long.parseLong(args[Ingester.FILESIZE_ARG_INDEX]),
                        receivedIdentifyRequestMessage.getAuditTrailInformation(),
                        TEST_CLIENT_ID
                ));

        addStep("Send a identify response to from the pillar to the client.",
                "A PutFileRequest should be received.");

        PutFileRequest receivedPutFileRequest = null;
        if(useMockupPillar()) {
            IdentifyPillarsForPutFileResponse identifyResponse = messageFactory
                    .createIdentifyPillarsForPutFileResponse(
                            receivedIdentifyRequestMessage, PILLAR1_ID, pillar1DestinationId);
            messageBus.sendMessage(identifyResponse);
            receivedPutFileRequest = pillar1Destination.waitForMessage(PutFileRequest.class, 10, TimeUnit.SECONDS);
            Assert.assertEquals(receivedPutFileRequest,
                    messageFactory.createPutFileRequest(
                            PILLAR1_ID, pillar1DestinationId,
                            receivedPutFileRequest.getReplyTo(),
                            receivedPutFileRequest.getCorrelationID(),
                            receivedPutFileRequest.getFileAddress(),
                            receivedPutFileRequest.getFileSize(),
                            args[Ingester.FILEID_ARG_INDEX],
                            receivedPutFileRequest.getAuditTrailInformation(),
                            TEST_CLIENT_ID
                    ));
        }

        addStep("Send a final response message to the client.",
                "The call to the main method should return with code 0. " +
                "The following json output should have been produced on standard out");
        PutFileFinalResponse putFileFinalResponse = messageFactory.createPutFileFinalResponse(
                receivedPutFileRequest, PILLAR1_ID, pillar1DestinationId);
        messageBus.sendMessage(putFileFinalResponse);
    }
}
