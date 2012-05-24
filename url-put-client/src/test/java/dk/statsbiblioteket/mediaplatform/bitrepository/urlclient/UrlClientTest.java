package dk.statsbiblioteket.mediaplatform.bitrepository.urlclient;

import org.bitrepository.client.DefaultFixtureClientTest;
import org.testng.annotations.Test;

public class UrlClientTest extends DefaultFixtureClientTest {

    @Test
    public void testClient() {
//            addDescription("Set the GetClient to retrieve a file as fast as "
//                    + "possible, where it has to choose between to pillars with "
//                    + "different times. The messages should be delivered at the "
//                    + "same time.");
//            addStep("Create a GetFileClient configured to use a fast and a slow pillar.", "");
//
//            String averagePillarID = "THE-AVERAGE-PILLAR";
//            String fastPillarID = "THE-FAST-PILLAR";
//            String slowPillarID = "THE-SLOW-PILLAR";
//            settings.getCollectionSettings().getClientSettings().getPillarIDs().clear();
//            settings.getCollectionSettings().getClientSettings().getPillarIDs().add(averagePillarID);
//            settings.getCollectionSettings().getClientSettings().getPillarIDs().add(fastPillarID);
//            settings.getCollectionSettings().getClientSettings().getPillarIDs().add(slowPillarID);
//            GetFileClient client = createGetFileClient();
//            TestEventHandler testEventHandler = new TestEventHandler(testEventManager);
//
//            addStep("Defining the variables for the GetFileClient and defining them in the configuration",
//                    "It should be possible to change the values of the configurations.");
//
//            addStep("Make the GetClient ask for fastest pillar.",
//                    "It should send message to identify which pillars and a IdentifyPillarsRequestSent notification should be generated.");
//            client.getFileFromFastestPillar(DEFAULT_FILE_ID, httpServer.getURL(DEFAULT_FILE_ID), testEventHandler);
//            IdentifyPillarsForGetFileRequest receivedIdentifyRequestMessage = null;
//            if (useMockupPillar()) {
//                receivedIdentifyRequestMessage =
//                        collectionDestination.waitForMessage(IdentifyPillarsForGetFileRequest.class);
//            }
//            Assert.assertEquals(testEventHandler.waitForEvent().getType(), OperationEvent.OperationEventType.IDENTIFY_REQUEST_SENT);
//
//            addStep("Three pillars send responses. First an average timeToDeliver, then a fast timeToDeliver and last a" +
//                    " slow timeToDeliver.", "The client should send a getFileRequest to the fast pillar. " +
//                    "The event handler should receive the following events: " +
//                    "3 x PillarIdentified, a PillarSelected and a RequestSent");
//
//            if (useMockupPillar()) {
//                IdentifyPillarsForGetFileResponse averageReply = testMessageFactory.createIdentifyPillarsForGetFileResponse(
//                        receivedIdentifyRequestMessage, averagePillarID, pillar2DestinationId);
//                TimeMeasureTYPE averageTime = new TimeMeasureTYPE();
//                averageTime.setTimeMeasureUnit(TimeMeasureUnit.MILLISECONDS);
//                averageTime.setTimeMeasureValue(BigInteger.valueOf(100L));
//                averageReply.setTimeToDeliver(averageTime);
//                messageBus.sendMessage(averageReply);
//
//                IdentifyPillarsForGetFileResponse fastReply = testMessageFactory.createIdentifyPillarsForGetFileResponse(
//                        receivedIdentifyRequestMessage, fastPillarID, pillar1DestinationId);
//                TimeMeasureTYPE fastTime = new TimeMeasureTYPE();
//                fastTime.setTimeMeasureUnit(TimeMeasureUnit.MILLISECONDS);
//                fastTime.setTimeMeasureValue(BigInteger.valueOf(10L));
//                fastReply.setTimeToDeliver(fastTime);
//                messageBus.sendMessage(fastReply);
//
//                IdentifyPillarsForGetFileResponse slowReply = testMessageFactory.createIdentifyPillarsForGetFileResponse(
//                        receivedIdentifyRequestMessage, slowPillarID, pillar2DestinationId);
//                TimeMeasureTYPE slowTime = new TimeMeasureTYPE();
//                slowTime.setTimeMeasureValue(BigInteger.valueOf(1L));
//                slowTime.setTimeMeasureUnit(TimeMeasureUnit.HOURS);
//                slowReply.setTimeToDeliver(slowTime);
//                messageBus.sendMessage(slowReply);
//
//                GetFileRequest receivedGetFileRequest = pillar1Destination.waitForMessage(GetFileRequest.class);
//                Assert.assertEquals(receivedGetFileRequest,
//                        testMessageFactory.createGetFileRequest(receivedGetFileRequest, fastPillarID,
//                                pillar1DestinationId, TEST_CLIENT_ID));
//            }
//
//            Assert.assertEquals(testEventHandler.waitForEvent().getType(), OperationEvent.OperationEventType.COMPONENT_IDENTIFIED);
//            Assert.assertEquals(testEventHandler.waitForEvent().getType(), OperationEvent.OperationEventType.COMPONENT_IDENTIFIED);
//            Assert.assertEquals(testEventHandler.waitForEvent().getType(), OperationEvent.OperationEventType.COMPONENT_IDENTIFIED);
//            ContributorEvent event = (ContributorEvent) testEventHandler.waitForEvent();
//            Assert.assertEquals(event.getType(), OperationEvent.OperationEventType.IDENTIFICATION_COMPLETE);
//            Assert.assertEquals(event.getContributorID(), fastPillarID);
//            Assert.assertEquals(testEventHandler.waitForEvent().getType(), OperationEvent.OperationEventType.REQUEST_SENT);
//        }
//        System.out.println("Running unit test");
    }
}
