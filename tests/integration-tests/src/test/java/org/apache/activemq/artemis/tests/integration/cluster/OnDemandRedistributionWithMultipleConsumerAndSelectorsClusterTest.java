package org.apache.activemq.artemis.tests.integration.cluster;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.tests.util.JMSClusteredTestBase;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify redistribution where there are several (local and remote) consumers with message filter.
 */
public class OnDemandRedistributionWithMultipleConsumerAndSelectorsClusterTest extends JMSClusteredTestBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // the "jms." prefix is needed because the cluster connection is matching on this
    public static final String QUEUE = "jms.Q1";

    /**
     * Configure the cluster connections with on-demand message load balancing.
     */
    @Override
    protected Configuration createConfigServer(final int source, final int destination) throws Exception {
        final String destinationLabel = "toServer" + destination;
        final String sourceLabel = "server" + source;

        Configuration configuration = createDefaultInVMConfig(source).setSecurityEnabled(false)
                .setJMXManagementEnabled(true)
                .setPersistenceEnabled(false)
                .addConnectorConfiguration(destinationLabel, new TransportConfiguration(InVMConnectorFactory.class.getName(), generateInVMParams(destination)))
                .addConnectorConfiguration(sourceLabel, new TransportConfiguration(InVMConnectorFactory.class.getName(), generateInVMParams(source)))
                .addClusterConfiguration(new ClusterConnectionConfiguration().setName(destinationLabel)
                        .setConnectorName(sourceLabel)
                        .setRetryInterval(250)
                        .setMaxHops(MAX_HOPS)
                        .setConfirmationWindowSize(1024)
                        .setMessageLoadBalancingType(MessageLoadBalancingType.ON_DEMAND)
                        .setStaticConnectors(new ArrayList<String>(List.of(destinationLabel))));

        configuration.getAddressSettings().put("#", new AddressSettings().setRedistributionDelay(0));

        return configuration;
    }

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

    private void enableTracing() {
        String[] loggers = {
                "org.apache.activemq.artemis.core.server.impl.QueueImpl",
                "org.apache.activemq.artemis.core.server.cluster.impl.Redistributor",
                "org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl"};
        for (String loggerName : loggers) {
            Configurator.setLevel(loggerName, Level.INFO);
        }
    }

    /**
     * Test redistribution of a message from server1 to server2:
     * - no local consumer on server1
     * - one remote consumer on server2 with a matching filter
     */
    @Test
    public void testRedistributionNoLocalConsumer() throws Exception {

        //
        // Configuration setup
        //

        createQueue(QUEUE);
        Queue jmsQueue = (Queue) context1.lookup("queue/" + QUEUE);

        // Activate redistribution with a delay of 1 second
        server1.getAddressSettingsRepository().getMatch("#").setRedistributionDelay(1000);
        server2.getAddressSettingsRepository().getMatch("#").setRedistributionDelay(1000);

        //
        // Execute test
        //

        log.info(">>> start connections [server1, server2]");
        Connection conn1 = cf1.createConnection();
        Connection conn2 = cf2.createConnection();
        conn1.start();
        conn2.start();

        try {
            Session session1 = conn1.createSession(Session.SESSION_TRANSACTED);
            Session session2 = conn2.createSession(Session.SESSION_TRANSACTED);

            log.info(">>> send message with header N=10 [server1]");
            MessageProducer prod1 = session1.createProducer(jmsQueue);
            prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
            TextMessage textMessage = session1.createTextMessage("m1");
            textMessage.setIntProperty("N", 10);
            prod1.send(textMessage);
            session1.commit();

            log.info(">>> create remote consumer with a matching filter [server2]");
            MessageConsumer cons2 = session2.createConsumer(jmsQueue, "N = 10");
            waitForBindings(server1, jmsQueue.getQueueName(), false, 1, 1, 4000);

            // Message should be redistributed to server2 after redistribution delay of 1 second

            log.info(">>> receive message, timeout longer than redistribution delay [server2]");
            TextMessage received = (TextMessage) cons2.receive(5000);
            assertNotNull(received);
            assertEquals("m1", received.getText());
            log.info(">>> commit message");
            session2.commit();

            // Seems to need an interrupt to update messageCount
            Thread.sleep(1);

            log.info(">>> verify message is gone from both servers");
            assertEquals(0, server1.locateQueue(jmsQueue.getQueueName()).getMessageCount());
            assertEquals(0, server2.locateQueue(jmsQueue.getQueueName()).getMessageCount());

        } finally {
            conn1.close();
            conn2.close();
        }
    }

    /**
     * Test redistribution of a message from server1 to server2:
     * - one local consumer with a negative filter on server1
     * - one remote consumer on server2 with a matching filter
     */
    @Test
    public void testRedistributionOneInitialLocalConsumer() throws Exception {
        //
        // Configuration setup
        //

        createQueue(QUEUE);
        Queue jmsQueue = (Queue) context1.lookup("queue/" + QUEUE);

        // Activate redistribution with a delay of 1 second
        server1.getAddressSettingsRepository().getMatch("#").setRedistributionDelay(1000);
        server2.getAddressSettingsRepository().getMatch("#").setRedistributionDelay(1000);

        //
        // Execute test
        //

        log.info(">>> start connections [server1, server2]");
        Connection conn1 = cf1.createConnection();
        Connection conn2 = cf2.createConnection();
        conn1.start();
        conn2.start();

        try {
            Session session1 = conn1.createSession(Session.SESSION_TRANSACTED);
            Session session2 = conn2.createSession(Session.SESSION_TRANSACTED);

            log.info(">>> send message with header N=10 [server1]");
            MessageProducer prod1 = session1.createProducer(jmsQueue);
            prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
            TextMessage textMessage = session1.createTextMessage("m1");
            textMessage.setIntProperty("N", 10);
            prod1.send(textMessage);
            session1.commit();

            enableTracing();

            log.info(">>> create local consumer with a negative matching filter [server1]");
            MessageConsumer cons1 = session1.createConsumer(jmsQueue, "N = 0");
            waitForBindings(server1, jmsQueue.getQueueName(), true, 1, 1, 4000);

            log.info(">>> create remote consumer with a matching filter [server2]");
            MessageConsumer cons2 = session2.createConsumer(jmsQueue, "N = 10");
            waitForBindings(server1, jmsQueue.getQueueName(), false, 1, 1, 4000);

            // Message should be redistributed to server2 after redistribution delay of 1 second

            log.info(">>> receive message, timeout longer than redistribution delay [server2]");
            TextMessage received = (TextMessage) cons2.receive(5000);
            assertNotNull(received);
            assertEquals("m1", received.getText());
            log.info(">>> commit message");
            session2.commit();

            // Seems to need an interrupt to update messageCount
            Thread.sleep(1);

            log.info(">>> verify message is gone from both servers");
            assertEquals(0, server1.locateQueue(jmsQueue.getQueueName()).getMessageCount());
            assertEquals(0, server2.locateQueue(jmsQueue.getQueueName()).getMessageCount());

        } finally {
            conn1.close();
            conn2.close();
        }
    }

    /**
     * Test redistribution of a message from server1 to server2:
     * - two local consumers with a negative filter on server1
     * - one remote consumer on server2 with a matching filter
     *
     *  ! The message is never redistributed to server2.
     */
    @Test
    public void testRedistributionManyInitialLocalConsumer() throws Exception {
        //
        // Configuration setup
        //

        createQueue(QUEUE);
        Queue jmsQueue = (Queue) context1.lookup("queue/" + QUEUE);

        // Activate redistribution with a delay of 1 second
        server1.getAddressSettingsRepository().getMatch("#").setRedistributionDelay(1000);
        server2.getAddressSettingsRepository().getMatch("#").setRedistributionDelay(1000);

        //
        // Execute test
        //

        log.info(">>> start connections [server1, server2]");
        Connection conn1 = cf1.createConnection();
        Connection conn2 = cf2.createConnection();
        conn1.start();
        conn2.start();

        try {
            Session session1 = conn1.createSession(Session.SESSION_TRANSACTED);
            Session session2 = conn2.createSession(Session.SESSION_TRANSACTED);

            log.info(">>> send message with header N=10 [server1]");
            MessageProducer prod1 = session1.createProducer(jmsQueue);
            prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
            TextMessage textMessage = session1.createTextMessage("m1");
            textMessage.setIntProperty("N", 10);
            prod1.send(textMessage);
            session1.commit();

            enableTracing();

            log.info(">>> create local consumer #1 with a negative matching filter [server1]");
            MessageConsumer cons1_1 = session1.createConsumer(jmsQueue, "N = 0");
            waitForBindings(server1, jmsQueue.getQueueName(), true, 1, 1, 4000);

            // Give some time for the consumer to be fully registered before creating the second consumer
            Thread.sleep(2000);

            log.info(">>> create local consumer #2 with a negative matching filter [server1]");
            MessageConsumer cons1_2 = session1.createConsumer(jmsQueue, "N = 0");
            waitForBindings(server1, jmsQueue.getQueueName(), true, 1, 2, 4000);

            // Give some time for the consumer to be fully registered before creating the second consumer
            Thread.sleep(2000);

            log.info(">>> create remote consumer with a matching filter [server2]");
            MessageConsumer cons2 = session2.createConsumer(jmsQueue, "N = 10");
            waitForBindings(server1, jmsQueue.getQueueName(), false, 1, 1, 4000);

            // Expected: Message should be redistributed to server2 after redistribution delay of 1 second
            // Actual: Message is not redistributed to server2
            // Because QueueImpl::hasUnMatchedPending is false, if there are more than one local consumers with negative matching filters.

            log.info(">>> receive message, timeout longer than redistribution delay [server2]");
            TextMessage received = (TextMessage) cons2.receive(5000);
            assertNotNull(received);
            assertEquals("m1", received.getText());
            log.info(">>> commit message");
            session2.commit();

            // Seems to need an interrupt to update messageCount
            Thread.sleep(1);

            log.info(">>> verify message is gone from both servers");
            assertEquals(0, server1.locateQueue(jmsQueue.getQueueName()).getMessageCount());
            assertEquals(0, server2.locateQueue(jmsQueue.getQueueName()).getMessageCount());

        } finally {
            conn1.close();
            conn2.close();
        }
    }

    /**
     * Test redistribution of a message from server1 to server2:
     * - one remote consumer on server2 with a matching filter
     * - one local consumers with a negative filter on server1, is created before RedistributionDelay expires
     *
     *  ! The message is not redistributed to server2 (canceled before the message is redistributed)
     */
    @Test
    public void testRedistributionLocalConsumerCancel() throws Exception {
        //
        // Configuration setup
        //

        createQueue(QUEUE);
        Queue jmsQueue = (Queue) context1.lookup("queue/" + QUEUE);

        // Activate redistribution with a delay of 1 second
        server1.getAddressSettingsRepository().getMatch("#").setRedistributionDelay(1000);
        server2.getAddressSettingsRepository().getMatch("#").setRedistributionDelay(1000);

        //
        // Execute test
        //

        log.info(">>> start connections [server1, server2]");
        Connection conn1 = cf1.createConnection();
        Connection conn2 = cf2.createConnection();
        conn1.start();
        conn2.start();

        try {
            Session session1 = conn1.createSession(Session.SESSION_TRANSACTED);
            Session session2 = conn2.createSession(Session.SESSION_TRANSACTED);

            log.info(">>> send message with header N=10 [server1]");
            MessageProducer prod1 = session1.createProducer(jmsQueue);
            prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
            TextMessage textMessage = session1.createTextMessage("m1");
            textMessage.setIntProperty("N", 10);
            prod1.send(textMessage);
            session1.commit();

            enableTracing();

            log.info(">>> create remote consumer with a matching filter [server2]");
            MessageConsumer cons2 = session2.createConsumer(jmsQueue, "N = 10");
            waitForBindings(server1, jmsQueue.getQueueName(), false, 1, 1, 4000);

            // Before the message is redistributed (delay 1 second), create a local consumer with a negative filter on server1

            log.info(">>> create local consumer with a negative matching filter [server1]");
            MessageConsumer cons1 = session1.createConsumer(jmsQueue, "N = 0");
            waitForBindings(server1, jmsQueue.getQueueName(), true, 1, 1, 4000);

            // This seems to cancel the delayed redistribution, and the message will stay on server1, even though it doesn't match the local consumer's filter

            log.info(">>> try to receive message on remote consumer, timeout longer than redistribution delay [server2]");
            TextMessage received = (TextMessage) cons2.receive(5000);
            assertNull(received);
            log.info(">>> no message received on remote consumer");

            // Seems to need an interrupt to update messageCount
            Thread.sleep(1);

            log.info(">>> verify message is still on server1");
            assertEquals(1, server1.locateQueue(jmsQueue.getQueueName()).getMessageCount());
            assertEquals(0, server2.locateQueue(jmsQueue.getQueueName()).getMessageCount());

        } finally {
            conn1.close();
            conn2.close();
        }
    }
}
