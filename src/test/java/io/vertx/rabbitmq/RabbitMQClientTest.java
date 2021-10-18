/*
 * Copyright 2021 Eclipse.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vertx.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.rabbitmq.impl.RabbitMQConnectionImpl;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;



/**
 *
 * @author jtalbut
 */
@RunWith(VertxUnitRunner.class)
public class RabbitMQClientTest {
  
  @SuppressWarnings("constantname")
  private static final Logger logger = LoggerFactory.getLogger(RabbitMQClientTest.class);
  
  @ClassRule
  public static final GenericContainer rabbitmq = new GenericContainer("rabbitmq:3.7-management")
    .withExposedPorts(5672, 15672);
  
  @Rule
  public RunTestOnContext testRunContext = new RunTestOnContext();

  @Test  
  public void testCreateWithWorkingServer(TestContext context) {
    RabbitMQOptions config = config();
    RabbitMQConnection connection = RabbitMQClient.create(testRunContext.vertx(), config);

    RabbitMQChannel channel = connection.createChannel();
    Async async = context.async();
    channel.connect()
            .onComplete(ar -> {
              if (ar.succeeded()) {
                logger.info("Completing test");
                async.complete();
              } else {
                logger.info("Failing test");
                context.fail(ar.cause());
              }
            });
    logger.info("Ending test");
  }
  
  private static final class TestConsumer implements Consumer {
    
    private final TestContext testContext;
    private final Async async;

    public TestConsumer(TestContext testContext, Async async) {
      this.testContext = testContext;
      this.async = async;
    }
    
    @Override
    public void handleConsumeOk(String consumerTag) {
    }

    @Override
    public void handleCancelOk(String consumerTag) {
    }

    @Override
    public void handleCancel(String consumerTag) throws IOException {
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
    }

    @Override
    public void handleRecoverOk(String consumerTag) {
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
      logger.info("Message received");
      testContext.assertEquals("Hello", new String(body, StandardCharsets.UTF_8));
      async.complete();
    }

  }
  
  @Test  
  public void testSendMessageWithWorkingServer(TestContext context) {
    RabbitMQOptions config = config();
    RabbitMQConnection connection = RabbitMQClient.create(testRunContext.vertx(), config);

    Async async = context.async();
    
    String exchange = "testSendMessageWithWorkingServer";
    String queue = "testSendMessageWithWorkingServerQueue";
    
    RabbitMQChannel conChan = connection.createChannel();
    RabbitMQChannel pubChan = connection.createChannel();
    conChan.exchangeDeclare(exchange, BuiltinExchangeType.FANOUT, true, false, null)
            .compose(v -> conChan.queueDeclare(queue, true, false, true, null))
            .compose(v -> conChan.queueBind(queue, exchange, "", null))
            .compose(v -> conChan.basicConsume(queue, new TestConsumer(context, async)))
            .compose(v -> pubChan.exchangeDeclare(exchange, BuiltinExchangeType.FANOUT, true, false, null))
            .compose(v -> pubChan.basicPublish(exchange, "", true, new BasicProperties(), "Hello".getBytes(StandardCharsets.UTF_8)))
            .onFailure(ex -> {
              logger.info("Failing test: ", ex);
              context.fail(ex);
            })
            ;
        
    logger.info("Ending test");
  }
  
  private int findOpenPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
  
  @Test
  public void testCreateWithServerThatArrivesLate(TestContext context) throws IOException {    
    RabbitMQOptions config = new RabbitMQOptions();
    config.setReconnectInterval(100);
    config.setReconnectAttempts(1000);
    RabbitMQConnectionImpl connection = (RabbitMQConnectionImpl) RabbitMQClient.create(testRunContext.vertx(), config);

    int port = findOpenPort();    
    GenericContainer container = new FixedHostPortGenericContainer("rabbitmq:3.7-management")
            .withFixedExposedPort(port, 5672)
            ;
    config.setUri("amqp://" + container.getContainerIpAddress() + ":" + port);

    testRunContext.vertx().setTimer(1000, time -> {
      container.start();
    });
    RabbitMQChannel channel = connection.createChannel();
    Async async = context.async();
    channel.connect()
            .onComplete(ar -> {
              if (ar.succeeded()) {
                logger.info("Completing test");
                context.assertTrue(connection.getReconnectCount() > 1);
                async.complete();
              } else {
                logger.info("Failing test");
                context.fail(ar.cause());
              }
            });
    logger.info("Ending test");    
  }
  
  public RabbitMQOptions config() {
    RabbitMQOptions config = new RabbitMQOptions();
    config.setUri("amqp://" + rabbitmq.getContainerIpAddress() + ":" + rabbitmq.getMappedPort(5672));
    return config;
  }

}
