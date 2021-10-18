package io.vertx.rabbitmq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;


@RunWith(VertxUnitRunner.class)
public class RabbitMQClientBuiltInReconnectTest {
  
  @SuppressWarnings("constantname")
  private static final Logger logger = LoggerFactory.getLogger(RabbitMQClientBuiltInReconnectTest.class);

  /**
   * This test verifies that the RabbitMQ Java client reconnection logic works as long as the vertx reconnect attempts is set to zero.
   *
   * The change that makes this work is in the basicConsumer, where the shutdown handler is only set if retries > 0. 
   * Without that change the vertx client shutdown handler is called, 
   * interrupting the java client reconnection logic, even though the vertx reconnection won't work because retries is zero.
   *
   */
  private static final String TEST_EXCHANGE = "RabbitMQClientBuiltInReconnectExchange";
  private static final String TEST_QUEUE = "RabbitMQClientBuiltInReconnectQueue";
  private static final boolean DEFAULT_RABBITMQ_EXCHANGE_DURABLE = false;
  private static final boolean DEFAULT_RABBITMQ_EXCHANGE_AUTO_DELETE = true;
  private static final BuiltinExchangeType DEFAULT_RABBITMQ_EXCHANGE_TYPE = BuiltinExchangeType.FANOUT;
  private static final boolean DEFAULT_RABBITMQ_QUEUE_DURABLE = false;
  private static final boolean DEFAULT_RABBITMQ_QUEUE_EXCLUSIVE = true;
  private static final boolean DEFAULT_RABBITMQ_QUEUE_AUTO_DELETE = true;

  private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQClientBuiltInReconnectTest.class);

  private final ObjectMapper mapper;
  private final Network network;
  private final GenericContainer networkedRabbitmq;
  private final ToxiproxyContainer toxiproxy;
  
  private final Vertx vertx;
  private final RabbitMQConnection connection;

  private final Set<Long> receivedMessages = new HashSet<>();
  
  private final Promise<Void> firstMessageReceived = Promise.promise();
  private final AtomicBoolean hasShutdown = new AtomicBoolean(false);
  private final Promise<Void> messageSentAfterShutdown = Promise.promise();
  private final Promise<Long> allMessagesSent = Promise.promise();
  private final Promise<Long> allMessagesReceived = Promise.promise();
  
  public RabbitMQClientBuiltInReconnectTest() {
    LOGGER.info("Constructing");
    this.mapper = new ObjectMapper();
    this.network = Network.newNetwork();
    this.networkedRabbitmq = new GenericContainer(DockerImageName.parse("rabbitmq:3.8.6-alpine"))
            .withExposedPorts(5672)
            .withNetwork(network);
    this.toxiproxy = new ToxiproxyContainer(DockerImageName.parse("shopify/toxiproxy:2.1.0"))
            .withNetwork(network);
    this.vertx = Vertx.vertx();
    this.connection = RabbitMQClient.create(vertx, getRabbitMQOptions());
  }

  private RabbitMQOptions getRabbitMQOptions() {
    RabbitMQOptions options = new RabbitMQOptions();

    ToxiproxyContainer.ContainerProxy proxy = toxiproxy.getProxy(networkedRabbitmq, 5672);
    options.setHost(proxy.getContainerIpAddress());
    options.setPort(proxy.getProxyPort());
    options.setConnectionTimeout(1000);
    options.setNetworkRecoveryInterval(1000);
    options.setRequestedHeartbeat(1);
    // Enable Java RabbitMQ client library reconnections
    options.setAutomaticRecoveryEnabled(true);
    // Disable vertx RabbitMQClient reconnections
    options.setReconnectAttempts(0);
    return options;
  }
  
  @Before
  public void setup() {
    LOGGER.info("Starting");
    this.networkedRabbitmq.start();
    this.toxiproxy.start();
  }

  @After
  public void shutdown() {
    this.networkedRabbitmq.stop();
    this.toxiproxy.stop();
    LOGGER.info("Shutdown");
  }

  @Test(timeout = 1 * 60 * 1000L)
  public void testRecoverConnectionOutage(TestContext ctx) throws Exception {
    Vertx vertx = Vertx.vertx();
    
    Async async = ctx.async();
    
    createAndStartConsumer(vertx);
    createAndStartProducer(vertx);
    
    // Have to react to allMessagesSent completing in case it completes after the last message is received.
    allMessagesSent.future().onSuccess(count -> {
      synchronized(receivedMessages) {
        if (receivedMessages.size() == count) {
          allMessagesReceived.tryComplete();
        }
      }
    });
    
    firstMessageReceived.future()
            .compose(v -> breakConnection())
            .compose(v -> messageSentAfterShutdown.future())
            .compose(v -> reestablishConnection())
            .compose(v -> allMessagesSent.future())
            .compose(v -> allMessagesReceived.future())
            .onComplete(ar -> {
              if (ar.succeeded()) {
                async.complete();
              } else {
                ctx.fail(ar.cause());
              }
            })
            ;

  }

  private void createAndStartProducer(Vertx vertx) {
    RabbitMQChannel channel = connection.createChannel();
   
    channel.addChannelEstablishedCallback(p -> {
      channel.exchangeDeclare(TEST_EXCHANGE, DEFAULT_RABBITMQ_EXCHANGE_TYPE, DEFAULT_RABBITMQ_EXCHANGE_DURABLE, DEFAULT_RABBITMQ_EXCHANGE_AUTO_DELETE, null)
              .onComplete(p);
    });
    RabbitMQPublisher publisher = channel.publish(TEST_EXCHANGE);
    AtomicLong counter = new AtomicLong();
    AtomicLong postShutdownCount = new AtomicLong(60);
    AtomicLong timerId = new AtomicLong();
    /*
    Send a message every second, with the message being a strictly increasing integer.
    After sending the first message when 'hasShutdown' is set complete the messageSentAfterShutdown to notify the main test.
    Then continue to send a further postShutdownCount messages, before cancelling the periodic timer and completing allMessagesSent with the total count of messages sent.
    */
    timerId.set(vertx.setPeriodic(1000, v -> {
      publisher.publish("", new BasicProperties(), Buffer.buffer(Long.toString(counter.incrementAndGet())));
      if (hasShutdown.get()) {
        messageSentAfterShutdown.tryComplete();
        if (postShutdownCount.decrementAndGet() == 0) {
          vertx.cancelTimer(timerId.get());
          allMessagesSent.complete(counter.get());
        }
      }
    }));
  }

  private void createAndStartConsumer(Vertx vertx) {
    RabbitMQChannel channel = connection.createChannel();
   
    channel.addChannelEstablishedCallback(p -> {
      channel.exchangeDeclare(TEST_EXCHANGE, DEFAULT_RABBITMQ_EXCHANGE_TYPE, DEFAULT_RABBITMQ_EXCHANGE_DURABLE, DEFAULT_RABBITMQ_EXCHANGE_AUTO_DELETE, null)
              .compose(v -> channel.queueDeclare(TEST_QUEUE, DEFAULT_RABBITMQ_QUEUE_DURABLE, DEFAULT_RABBITMQ_QUEUE_EXCLUSIVE, DEFAULT_RABBITMQ_QUEUE_AUTO_DELETE, null))
              .compose(v -> channel.queueBind(TEST_QUEUE, TEST_EXCHANGE, "", null))
              .onComplete(p);
    });
    RabbitMQConsumer consumer = channel.consume(TEST_QUEUE);
    consumer.handler(message -> {
      Long index = Long.parseLong(message.body().toString(StandardCharsets.UTF_8));
      firstMessageReceived.tryComplete();
      synchronized(receivedMessages) {
        receivedMessages.add(index);
        Future<Long> allMessagesSentFuture = allMessagesSent.future();
        if (allMessagesSentFuture.isComplete() && (receivedMessages.size() == allMessagesSentFuture.result())) {
          allMessagesReceived.tryComplete();
        }
      }
    });
  }

  private Future<Void> breakConnection() {
    return vertx.executeBlocking(promise -> {
      this.toxiproxy.stop();      
      promise.complete();      
    });
  }
  
  private Future<Void> reestablishConnection() {
    return vertx.executeBlocking(promise -> {
      this.toxiproxy.start();
      promise.complete();      
    });
  }
  
}
