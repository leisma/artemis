package org.apache.activemq.artemis.tests.integration.jms.cluster;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.tests.util.JMSClusteredTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

import java.lang.invoke.MethodHandles;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify redelivery count when the message is redistributed to another node in a cluster.
 *
 */
public class RedeliveryWithRedistributionClusterTest extends JMSClusteredTestBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // the "jms." prefix is needed because the cluster connection is matching on this
    public static final String QUEUE = "jms.Q1";
    public static final String DLQ_QUEUE = "jms.Q1_DLQ";

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        jmsServer1.getActiveMQServer().setIdentity("Server 1");
        jmsServer2.getActiveMQServer().setIdentity("Server 2");
    }

    @Override
    protected boolean enablePersistence() {
      return true;
   }

    /**
     * Test to verify how redelivery works without redistribution.
     * - one message is sent to server1
     * - consumer is created on server1
     * - consumer receives the message 5 times and makes a rollback to simulate a failure
     * - after MaxDeliveryAttempts the message is moved to the DLQ
     *
     * The message is never redistributed to server2.
     */
    @Test
    public void testRedeliveryCountMessageIsNotRedistributed() throws Exception {

        //
        // Configuration setup
        //

        // Create a Queue and a DLQ on both servers
        createQueue(QUEUE);
        createQueue(DLQ_QUEUE);
        Queue jmsQueue = (Queue) context1.lookup("queue/" + QUEUE);

        // Configure redelivery to retry 5 times with a delay of 100ms and then move to the DLQ
        server1.getAddressSettingsRepository().getMatch("#").setRedeliveryDelay(100);
        server1.getAddressSettingsRepository().getMatch("#").setMaxDeliveryAttempts(5);
        server1.getAddressSettingsRepository().getMatch("#").setDeadLetterAddress(SimpleString.of(DLQ_QUEUE));

        server2.getAddressSettingsRepository().getMatch("#").setRedeliveryDelay(100);
        server2.getAddressSettingsRepository().getMatch("#").setMaxDeliveryAttempts(5);
        server2.getAddressSettingsRepository().getMatch("#").setDeadLetterAddress(SimpleString.of(DLQ_QUEUE));

        // Activate redistribution with a delay of 1 second
        server1.getAddressSettingsRepository().getMatch("#").setRedistributionDelay(1000);
        server2.getAddressSettingsRepository().getMatch("#").setRedistributionDelay(1000);

        //
        // Execute test
        //

        log.info(">>> start connection [server1]");
        Connection conn1 = cf1.createConnection();
        conn1.start();

        try {
            Session session1 = conn1.createSession(Session.SESSION_TRANSACTED);

            log.info(">>> send message [server1]");
            MessageProducer prod1 = session1.createProducer(jmsQueue);
            prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
            TextMessage textMessage = session1.createTextMessage("m1");
            prod1.send(textMessage);
            session1.commit();

            log.info(">>> create consumer [server1]");
            MessageConsumer cons1 = session1.createConsumer(jmsQueue);
            waitForBindings(server1, jmsQueue.getQueueName(), true, 1, 1, 4000);

            //
            // Try to consume the message 5 times (MaxDeliveryAttempts), each time rolling back the session to simulate a failure
            for (int i = 1; i <= 5; i++) {
                log.info(">>> receive message #{} [server1]", i);
                TextMessage received = (TextMessage) cons1.receive(2000);
                assertNotNull(received);
                assertEquals("m1", received.getText());
                assertEquals(i, received.getIntProperty("JMSXDeliveryCount"));

                log.info(">>> simulate failure, rollback message #{} [server1]", i);
                session1.rollback();
            }

            //
            // MaxDeliveryAttempts reached, the message is moved to the DLQ and no longer available on the original queue

            log.info(">>> try to receive message after MaxDeliveryAttempts [server1]");
            TextMessage received = (TextMessage) cons1.receive(2000);
            assertNull(received);
            log.info(">>> No message received, moved to DLQ");

            // Seems to need an interrupt to update messageCount
            Thread.sleep(1);

            log.info(">>> verify message is moved to DLQ");
            assertEquals(1, server1.locateQueue(DLQ_QUEUE).getMessageCount());

            log.info(">>> verify message is gone from both servers");
            assertEquals(0, server1.locateQueue(jmsQueue.getQueueName()).getMessageCount());
            assertEquals(0, server2.locateQueue(jmsQueue.getQueueName()).getMessageCount());

        } finally {
            conn1.close();
        }
    }

    /**
     * Test to verify how redelivery works with redistribution, especially how the delivery count is handled.
     * - one message is sent to server1
     * - consumer1 is created on server1
     * - consumer1 receives the message 4 times and makes a rollback to simulate a failure
     * - MaxDeliveryAttempts not yet reached, the message is still on server1
     * - consumer1 is closed and a new consumer2 is created on server2
     * - the message is redistributed to server2
     *   - delivery count is reset when the message is redistributed to server2
     * - consumer2 receives the message 5 times and makes a rollback to simulate a failure
     * - after MaxDeliveryAttempts the message is moved to the DLQ
     *
     * @throws Exception
     */
    @Test
    public void testRedeliveryCountMessageIsRedistributed() throws Exception {
        //
        // Configuration setup
        //

        // Create a Queue and a DLQ on both servers
        createQueue(QUEUE);
        createQueue(DLQ_QUEUE);
        Queue jmsQueue = (Queue) context1.lookup("queue/" + QUEUE);

        // Configure redelivery to retry 5 times with a delay of 100ms and then move to the DLQ
        server1.getAddressSettingsRepository().getMatch("#").setRedeliveryDelay(100);
        server1.getAddressSettingsRepository().getMatch("#").setMaxDeliveryAttempts(5);
        server1.getAddressSettingsRepository().getMatch("#").setDeadLetterAddress(SimpleString.of(DLQ_QUEUE));

        server2.getAddressSettingsRepository().getMatch("#").setRedeliveryDelay(100);
        server2.getAddressSettingsRepository().getMatch("#").setMaxDeliveryAttempts(5);
        server2.getAddressSettingsRepository().getMatch("#").setDeadLetterAddress(SimpleString.of(DLQ_QUEUE));

        // Activate redistribution with a delay of 1 second
        server1.getAddressSettingsRepository().getMatch("#").setRedistributionDelay(1000);
        server2.getAddressSettingsRepository().getMatch("#").setRedistributionDelay(1000);

        //
        // Execute test
        //

        log.info(">>> start connections [server1,server2]");
        Connection conn1 = cf1.createConnection();
        Connection conn2 = cf2.createConnection();
        conn1.start();
        conn2.start();

        try {
            log.info(">>> create sessions [server1,server2]");
            Session session1 = conn1.createSession(Session.SESSION_TRANSACTED);
            Session session2 = conn2.createSession(Session.SESSION_TRANSACTED);

            log.info(">>> send message [server1]");
            MessageProducer prod1 = session1.createProducer(jmsQueue);
            prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
            TextMessage textMessage = session1.createTextMessage("m1");
            prod1.send(textMessage);
            session1.commit();

            log.info(">>> create consumer [server1]");
            MessageConsumer cons1 = session1.createConsumer(jmsQueue);
            waitForBindings(server1, jmsQueue.getQueueName(), true, 1, 1, 4000);

            //
            // Try to consume the message 4 times (before MaxDeliveryAttempts reached), each time rolling back the session to simulate a failure
            for (int i = 1; i <= 4; i++) {
                log.info(">>> receive message #{} [server1]", i);
                TextMessage received = (TextMessage) cons1.receive(2000);
                assertNotNull(received);
                assertEquals("m1", received.getText());
                assertEquals(i, received.getIntProperty("JMSXDeliveryCount"));

                log.info(">>> rollback message #{} [server1]", i);
                session1.rollback();
            }

            log.info(">>> close consumer on server1");
            cons1.close();
            waitForBindings(server1, jmsQueue.getQueueName(), true, 1, 0, 4000);

            log.info(">>> create consumer on server2");
            MessageConsumer cons2 = session2.createConsumer(jmsQueue);
            waitForBindings(server1, jmsQueue.getQueueName(), false, 1, 1, 4000);

            // Message should now be redistributed to server2
            // The delivery count is not preserved when the message is redistributed to another node in the cluster, it is reset to 0

            //
            // Try to consume the message 5 times (MaxDeliveryAttempts), each time rolling back the session to simulate a failure
            for (int i = 1; i <= 5; i++) {
                log.info(">>> receive message #{} [server2]", i);
                TextMessage received = (TextMessage) cons2.receive(2000);
                assertNotNull(received);
                assertEquals("m1", received.getText());

                // Delivery count is reset, started from the beginning
                assertEquals(i, received.getIntProperty("JMSXDeliveryCount"));

                log.info(">>> simulate failure, rollback message #{} [server2]", i);
                session2.rollback();
            }

            log.info(">>> receive message after MaxDeliveryAttempts [server2]");
            TextMessage received = (TextMessage) cons2.receive(2000);
            assertNull(received);
            log.info(">>> No message received, moved to DLQ");

            // Seems to need an interrupt to update messageCount
            Thread.sleep(1);

            log.info(">>> verify message is moved to DLQ");
            assertEquals(1, server2.locateQueue(DLQ_QUEUE).getMessageCount());

            log.info(">>> verify message is gone from both servers");
            assertEquals(0, server1.locateQueue(jmsQueue.getQueueName()).getMessageCount());
            assertEquals(0, server2.locateQueue(jmsQueue.getQueueName()).getMessageCount());

        } finally {
            conn1.close();
            conn2.close();
        }
    }
}
