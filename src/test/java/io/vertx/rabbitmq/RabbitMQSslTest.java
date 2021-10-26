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

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import static org.junit.Assert.assertNotNull;



/**
 *
 * @author jtalbut
 */
@RunWith(VertxUnitRunner.class)
public class RabbitMQSslTest {
  
  @SuppressWarnings("constantname")
  private static final Logger logger = LoggerFactory.getLogger(RabbitMQSslTest.class);
  
  @ClassRule
  public static final GenericContainer rabbitmq = new GenericContainer("rabbitmq:3.9.8-management-alpine")
          .withCopyFileToContainer(MountableFile.forClasspathResource("/ssl-server/rabbitmq.conf"), "/etc/rabbitmq/rabbitmq.conf")
          .withCopyFileToContainer(MountableFile.forClasspathResource("/ssl-server/ca/ca_certificate.pem"), "/etc/rabbitmq/ca_certificate.pem")
          .withCopyFileToContainer(MountableFile.forClasspathResource("/ssl-server/server/server_certificate.pem"), "/etc/rabbitmq/server_certificate.pem")
          .withCopyFileToContainer(MountableFile.forClasspathResource("/ssl-server/server/private_key.pem"), "/etc/rabbitmq/server_key.pem")
          .withExposedPorts(5671, 5672, 15672);
  
  @Rule
  public RunTestOnContext testRunContext = new RunTestOnContext();

  public RabbitMQOptions config() {
    RabbitMQOptions config = new RabbitMQOptions();
    config.setUri("amqps://" + rabbitmq.getContainerIpAddress() + ":" + rabbitmq.getMappedPort(5671));
    config.setConnectionName(this.getClass().getSimpleName());
    config.setSsl(true);
    config.setTrustAll(true);
    config.setHostnameVerificationAlgorithm("");
    return config;
  }
  
  @Test
  public void rawLibTest() throws Exception {
    ConnectionFactory cf = new ConnectionFactory();
    cf.useNio();
    String uri = "amqps://" + rabbitmq.getContainerIpAddress() + ":" + rabbitmq.getMappedPort(5671);
    cf.setUri(uri);
    logger.info("Connecting to {} using default settings", uri);
    try (Connection conn = cf.newConnection("Test")) {
      assertNotNull(conn);
      logger.info("Connected to {}", uri);
      try (Channel chann = conn.createChannel()) {
        chann.exchangeDeclare("rawLibTest", BuiltinExchangeType.FANOUT);
        logger.info("Exchange declared");
      }
    }
  }
  
  @Test
  public void testCreateWithWorkingServer(TestContext context) {
    RabbitMQOptions config = config();
    RabbitMQConnection connection = RabbitMQClient.create(testRunContext.vertx(), config);

    RabbitMQChannel channel = connection.createChannel();
    Async async = context.async();
    channel.connect()
            .compose(v -> channel.exchangeDeclare("testCreateWithWorkingServer", BuiltinExchangeType.FANOUT, true, true, null))
            .onComplete(ar -> {
              if (ar.succeeded()) {
                logger.info("Exchange declared");
                logger.info("Completing test");
                connection.close().onComplete(ar2 -> {
                  async.complete();                     
                });
              } else {
                logger.info("Failing test");
                context.fail(ar.cause());
              }
            });
  }
  
}
