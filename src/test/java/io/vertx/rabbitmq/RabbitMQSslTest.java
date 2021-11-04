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

import io.vertx.core.net.JksOptions;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.test.tls.Trust;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;





/**
 *
 * @author jtalbut
 */
@RunWith(VertxUnitRunner.class)
public class RabbitMQSslTest {
  
  @SuppressWarnings("constantname")
  private static final Logger logger = LoggerFactory.getLogger(RabbitMQSslTest.class);
  
  @ClassRule
  public static final GenericContainer rabbitmq = RabbitMQBrokerProvider.getRabbitMqContainer();
  
  @Rule
  public RunTestOnContext testRunContext = new RunTestOnContext();

  protected static final JksOptions TRUSTED = new JksOptions().setPath(Trust.SERVER_JKS.get().getPath());
  protected static final JksOptions UN_TRUSTED = new JksOptions().setPath(Trust.CLIENT_JKS.get().getPath());
  
  public RabbitMQOptions config() {
    RabbitMQOptions config = new RabbitMQOptions();
    config.setUri("amqps://" + rabbitmq.getContainerIpAddress() + ":" + rabbitmq.getMappedPort(5671));
    config.setConnectionName(this.getClass().getSimpleName());
    config.setHostnameVerificationAlgorithm("");
    return config;
  }
  
  @Test
  public void doNothing() {    
  }
  
//  @Test
//  public void rawLibTestTrustAll() throws Exception {
//    ConnectionFactory cf = new ConnectionFactory();
//    cf.useNio();
//    String uri = "amqps://" + rabbitmq.getContainerIpAddress() + ":" + rabbitmq.getMappedPort(5671);
//    cf.setUri(uri);
//    logger.info("Connecting to {} using default settings", uri);
//    try (Connection conn = cf.newConnection("Test")) {
//      assertNotNull(conn);
//      logger.info("Connected to {}", uri);
//      try (Channel chann = conn.createChannel()) {
//        chann.exchangeDeclare("rawLibTest", BuiltinExchangeType.FANOUT);
//        logger.info("Exchange declared");
//      }
//    }
//  }
//  
//  @Test
//  public void rawLibTestKeyManager() throws Exception {
//    ConnectionFactory cf = new ConnectionFactory();
//    cf.useNio();
//    
//    KeyManagerFactory kmf = TRUSTED.getKeyManagerFactory(testRunContext.vertx());
//    TrustManagerFactory tmf = TRUSTED.getTrustManagerFactory(testRunContext.vertx());
//    SSLContext sslContext = SSLContext.getInstance("TLSv1.3");
//    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);      
//    cf.useSslProtocol(sslContext);
//    
//    String uri = "amqps://" + rabbitmq.getContainerIpAddress() + ":" + rabbitmq.getMappedPort(5671);
//    cf.setUri(uri);
//    logger.info("Connecting to {} using default settings", uri);
//    try (Connection conn = cf.newConnection("Test")) {
//      assertNotNull(conn);
//      logger.info("Connected to {}", uri);
//      try (Channel chann = conn.createChannel()) {
//        chann.exchangeDeclare("rawLibTest", BuiltinExchangeType.FANOUT);
//        logger.info("Exchange declared");
//      }
//    }
//  }
//  
//  @Test
//  public void testCreateWithInsecureServer(TestContext context) {
//    RabbitMQOptions config = config();
//    RabbitMQConnection connection = RabbitMQClient.create(testRunContext.vertx(), config);
//
//    RabbitMQChannel channel = connection.createChannel();
//    Async async = context.async();
//    channel.connect()
//            .compose(v -> channel.exchangeDeclare("testCreateWithWorkingServer", BuiltinExchangeType.FANOUT, true, true, null))
//            .onComplete(ar -> {
//              if (ar.succeeded()) {
//                logger.info("Exchange declared");
//                logger.info("Completing test");
//                connection.close().onComplete(ar2 -> {
//                  async.complete();                     
//                });
//              } else {
//                logger.info("Failing test");
//                context.fail(ar.cause());
//              }
//            });
//  }
//  
//  @Test(timeout = 20000L)
//  public void testCreateWithSecureServer(TestContext context) throws Exception {
//    RabbitMQOptions config = config();
//    config.setConnectionTimeout(1000);
//    config.setKeyStoreOptions(UN_TRUSTED);
//    RabbitMQConnection connection = RabbitMQClient.create(testRunContext.vertx(), config);
//
//    RabbitMQChannel channel = connection.createChannel();
//    Async async = context.async();
//    channel.connect()
//            .onComplete(ar -> {
//              if (ar.succeeded()) {
//                context.fail("Expected to fail");
//              } else {
//                async.complete();                     
//              }
//            });
//  }
  
}
