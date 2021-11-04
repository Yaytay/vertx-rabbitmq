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

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import static org.junit.Assert.assertNotNull;

/**
 *
 * @author jtalbut
 */
public class RabbitMQSslRawTest {
  
  @SuppressWarnings("constantname")
  private static final Logger logger = LoggerFactory.getLogger(RabbitMQSslRawTest.class);
  
  public static final GenericContainer rabbitmq = getRabbitMqContainer();  

  public static GenericContainer getRabbitMqContainer() {
    GenericContainer container = new GenericContainer("rabbitmq:3.9.8-management-alpine")
          .withCopyFileToContainer(MountableFile.forClasspathResource("/ssl-server/rabbitmq.conf"), "/etc/rabbitmq/rabbitmq.conf")
          .withCopyFileToContainer(MountableFile.forClasspathResource("/ssl-server/ca/ca_certificate.pem"), "/etc/rabbitmq/ca_certificate.pem")
          .withCopyFileToContainer(MountableFile.forClasspathResource("/ssl-server/server/server_certificate.pem"), "/etc/rabbitmq/server_certificate.pem")
          .withCopyFileToContainer(MountableFile.forClasspathResource("/ssl-server/server/private_key.pem"), "/etc/rabbitmq/server_key.pem")
          .withExposedPorts(5671, 5672, 15672)
                ;
    container.start();
    logger.info("Started test instance of RabbitMQ with ports {}"
            , container.getExposedPorts().stream().map(p -> Integer.toString((Integer) p) + ":" + Integer.toString(container.getMappedPort((Integer) p))).collect(Collectors.toList())
    );
    return container;
  }
  
  @Test
  public void testSsl() throws Throwable {
    
    char[] trustPassphrase = "password".toCharArray();
    KeyStore tks = KeyStore.getInstance("JKS");
    InputStream tustKeyStoreStream = this.getClass().getResourceAsStream("/ssl-server/localhost-test-rabbit-store");
    tks.load(tustKeyStoreStream, trustPassphrase);

    TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
    tmf.init(tks);

    SSLContext c = SSLContext.getInstance("TLSv1.3");
    c.init(null, tmf.getTrustManagers(), null);

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    factory.setPort(rabbitmq.getMappedPort(5671));
    factory.useSslProtocol(c);
    // factory.useNio();

    Connection conn = factory.newConnection();
    assertNotNull(conn);
    conn.close();
    logger.info("Connected");

  }

}
