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
package io.vertx.rabbitmq.impl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import io.netty.handler.ssl.JdkSslContext;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.net.JdkSSLEngineOptions;
import io.vertx.core.net.impl.SSLHelper;
import io.vertx.rabbitmq.RabbitMQChannel;
import io.vertx.rabbitmq.RabbitMQConnection;
import io.vertx.rabbitmq.RabbitMQOptions;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jtalbut
 */
public class RabbitMQConnectionImpl implements RabbitMQConnection, ShutdownListener {
  
  @SuppressWarnings("constantname")
  private static final Logger logger = LoggerFactory.getLogger(RabbitMQConnectionImpl.class);
  
  private final Vertx vertx;
  private final RabbitMQOptions config;
  
  private boolean connectedAtLeastOnce;
  private boolean established;
  private final Object connectingPromiseLock = new Object();
  private volatile Future<Connection> connectingFuture;
  private final Object connectionLock = new Object();
  private volatile Connection connection;

  private int reconnectCount;
  
  public RabbitMQConnectionImpl(Vertx vertx, RabbitMQOptions config) {
    this.vertx = vertx;
    this.config = config;
  }

  public boolean isEstablished() {
    return established;
  }

  public int getReconnectCount() {
    return reconnectCount;
  }
    
  private Connection rawConnect() throws IOException, TimeoutException {
    List<Address> addresses = null;
    ConnectionFactory cf = new ConnectionFactory();
    String uri = config.getUri();
    // Use uri if set, otherwise support individual connection parameters
    if (uri != null) {
      try {
        logger.info("Connecting to " + uri);
        cf.setUri(uri);
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid rabbitmq connection uri ", e);
      }
    } else {
      addresses = config.getAddresses().isEmpty()
        ? Collections.singletonList(new Address(config.getHost(), config.getPort()))
        : config.getAddresses();
      cf.setVirtualHost(config.getVirtualHost());
    }
    // Note that this intentionally allows the configuration to override properties from the URL.
    if (config.getUser() != null && !config.getUser().isEmpty()) {
      cf.setUsername(config.getUser());
    }
    if (config.getPassword() != null && !config.getPassword().isEmpty()) {
      cf.setPassword(config.getPassword());
    }
    if (config.getVirtualHost() != null && !config.getVirtualHost().isEmpty()) {
      cf.setVirtualHost(config.getVirtualHost());
    }
    
    cf.setConnectionTimeout(config.getConnectionTimeout());
    cf.setRequestedHeartbeat(config.getRequestedHeartbeat());
    cf.setHandshakeTimeout(config.getHandshakeTimeout());
    cf.setRequestedChannelMax(config.getRequestedChannelMax());
    cf.setNetworkRecoveryInterval(config.getNetworkRecoveryInterval());
    cf.setAutomaticRecoveryEnabled(config.isAutomaticRecoveryEnabled());

    if (config.isSsl()) {
      //The RabbitMQ Client connection needs a JDK SSLContext, so force this setting.
      config.setSslEngineOptions(new JdkSSLEngineOptions());
      SSLHelper sslHelper = new SSLHelper(config, config.getKeyCertOptions(), config.getTrustOptions());
      JdkSslContext ctx = (JdkSslContext) sslHelper.getContext((VertxInternal) vertx);
      cf.useSslProtocol(ctx.context());
    }

    cf.useNio();

    Connection conn = addresses == null
           ? cf.newConnection(config.getConnectionName())
           : cf.newConnection(addresses, config.getConnectionName());
    
    conn.addShutdownListener(this);
    
    return conn;
  }

  @Override
  public void shutdownCompleted(ShutdownSignalException cause) {
    logger.info("Connection Shutdown: {}", cause.getMessage());
  }
  
  protected boolean shouldRetryConnection() {
    if ((config.getReconnectInterval() > 0) 
            && ((config.getReconnectAttempts() < 0) || config.getReconnectAttempts() > reconnectCount)
            && (connectedAtLeastOnce || config.isReconnectOnInitialConnection())
            ) {
      ++reconnectCount;
      return true;
    } else {
      return false;
    }
  }
  
  private void connectBlocking(Promise<Connection> promise) {
    try {
      synchronized(connectionLock) {
        connection = rawConnect();
      }
      connectedAtLeastOnce = true;
      promise.complete(connection);
    } catch(Throwable ex) {
      logger.error("Failed to create connection: ", ex);
      if (shouldRetryConnection()) {
        vertx.setTimer(config.getReconnectInterval(), time -> connectBlocking(promise));
      } else {
        promise.fail(ex);
      }
    }
  }  
  
  public Future<Channel> openChannel() {
    synchronized(connectingPromiseLock) {
      if (connectingFuture == null) {
        reconnectCount = 0;
        Promise<Connection> connectingPromise = Promise.promise();
        connectingFuture = connectingPromise.future();
        vertx.executeBlocking(execPromise -> connectBlocking(connectingPromise));
      }
      return connectingFuture
              .compose(conn -> {
                return vertx.executeBlocking(promise -> {
                  try {
                    promise.complete(conn.createChannel());
                  } catch(IOException ex) {
                    logger.error("Failed to create channel: ", ex);
                    if (shouldRetryConnection()) {
                      openChannel().onComplete(promise);
                    }
                    promise.fail(ex);
                  }
                });
              });
    }
  }

  @Override
  public RabbitMQChannel createChannel() {
    return new RabbitMQChannelImpl(vertx, this);
  }

  @Override
  public Future<Void> abort(int closeCode, String closeMessage, int timeout) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Future<Void> close(int closeCode, String closeMessage, int timeout) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void close() throws Exception {
    close(AMQP.REPLY_SUCCESS, "OK", config.getHandshakeTimeout());
  }
  
}
