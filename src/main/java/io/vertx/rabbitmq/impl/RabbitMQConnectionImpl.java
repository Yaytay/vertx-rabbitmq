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
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.rabbitmq.RabbitMQChannel;
import io.vertx.rabbitmq.RabbitMQConnection;
import io.vertx.rabbitmq.RabbitMQOptions;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author jtalbut
 */
public class RabbitMQConnectionImpl implements RabbitMQConnection, ShutdownListener {
  
  private static final Logger logger = LoggerFactory.getLogger(RabbitMQConnectionImpl.class);
  
  private final Vertx vertx;
  private final Context context;
  private final RabbitMQOptions config;
  private String connectionName;
  
  private boolean connectedAtLeastOnce;
  private boolean established;
  private final Object connectingPromiseLock = new Object();
  private volatile Future<Connection> connectingFuture;
  private final Object connectionLock = new Object();
  private volatile Connection connection;  

  private int reconnectCount;
  private long lastConnectedInstance = -1;
  
  private final AtomicLong connectCount = new AtomicLong();
  private volatile boolean closed;
  
  public RabbitMQConnectionImpl(Vertx vertx, RabbitMQOptions config) {
    this.vertx = vertx;
    this.context = vertx.getOrCreateContext();    
    this.config = config;
  }

  @Override
  public long getConnectionInstance() {
    return connectCount.get();
  }
  
  public boolean isEstablished() {
    return established;
  }

  public int getReconnectCount() {
    return reconnectCount;
  }

  @Override
  public String getConnectionName() {
    return connectionName;
  }
  
  private Connection rawConnect() throws IOException, TimeoutException {
    List<Address> addresses = null;
    ConnectionFactory cf = new ConnectionFactory();
    String uriString = config.getUri();
    // Use uri if set, otherwise support individual connection parameters
    if (uriString != null) {      
      try {
        URI uri = new URI(uriString);
        cf.setUri(uri);
        if ("amqps".equals(uri.getScheme())) {
          configureSslProtocol(cf);
        }
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
    if (config.isSsl()) {
      configureSslProtocol(cf);
    }
    if (addresses != null) {
      logger.info("{}onnecting to amqp{}://{}@{}/{}"
              , connectCount.get() > 0 ? "Rec" : "C"
              , cf.isSSL() ? "s" : ""
              , cf.getUsername()
              , addresses.size() == 1 ? addresses.get(0) : addresses
              , URLEncoder.encode(cf.getVirtualHost(), "UTF-8")
      );
    } else {
      logger.info("{}onnecting to amqp{}://{}@{}:{}/{}"
              , connectCount.get() > 0 ? "Rec" : "C"
              , cf.isSSL() ? "s" : ""
              , cf.getUsername()
              , cf.getHost()
              , cf.getPort()
              , URLEncoder.encode(cf.getVirtualHost(), "UTF-8")
      );      
    }
    
    cf.setConnectionTimeout(config.getConnectionTimeout());
    cf.setShutdownTimeout(config.getShutdownTimeout());
    cf.setWorkPoolTimeout(config.getWorkPoolTimeout());
    cf.setRequestedHeartbeat(config.getRequestedHeartbeat());
    cf.setHandshakeTimeout(config.getHandshakeTimeout());
    cf.setRequestedChannelMax(config.getRequestedChannelMax());
    cf.setRequestedFrameMax(config.getRequestedFrameMax());
    cf.setNetworkRecoveryInterval(config.getNetworkRecoveryInterval());
    cf.setAutomaticRecoveryEnabled(config.isAutomaticRecoveryEnabled());
    if (config.getTopologyRecoveryEnabled() == null) {
      cf.setTopologyRecoveryEnabled(config.isAutomaticRecoveryEnabled());      
    } else {
      cf.setTopologyRecoveryEnabled(config.getTopologyRecoveryEnabled());
    }

    cf.useNio();

    cf.setChannelRpcTimeout(config.getChannelRpcTimeout());
    cf.setChannelShouldCheckRpcResponseType(config.isChannelShouldCheckRpcResponseType());
    cf.setClientProperties(config.getClientProperties());
    if (config.getConnectionRecoveryTriggeringCondition() != null) {
      cf.setConnectionRecoveryTriggeringCondition(config.getConnectionRecoveryTriggeringCondition());
    }
    if (config.getCredentialsProvider() != null) {
      cf.setCredentialsProvider(config.getCredentialsProvider());
    }
    if (config.getCredentialsRefreshService() != null) {
      cf.setCredentialsRefreshService(config.getCredentialsRefreshService());
    }
    if (config.getErrorOnWriteListener() != null) {
      cf.setErrorOnWriteListener(config.getErrorOnWriteListener());
    }
    if (config.getExceptionHandler() != null) {
      cf.setExceptionHandler(config.getExceptionHandler());
    }
    if (config.getHeartbeatExecutor() != null) {
      cf.setHeartbeatExecutor(config.getHeartbeatExecutor());
    }
    if (config.getMetricsCollector() != null) {
      cf.setMetricsCollector(config.getMetricsCollector());
    }
    if (config.getNioParams() != null) {
      cf.setNioParams(config.getNioParams());
    }
    if (config.getRecoveredQueueNameSupplier() != null) {
      cf.setRecoveredQueueNameSupplier(config.getRecoveredQueueNameSupplier());
    }
    if (config.getRecoveryDelayHandler() != null) {
      cf.setRecoveryDelayHandler(config.getRecoveryDelayHandler());
    }
    if (config.getSaslConfig() != null) {
      cf.setSaslConfig(config.getSaslConfig());
    }
    if (config.getSharedExecutor() != null) {
      cf.setSharedExecutor(config.getSharedExecutor());
    }
    if (config.getShutdownExecutor() != null) {
      cf.setShutdownExecutor(config.getShutdownExecutor());
    }
    if (config.getSocketConfigurator() != null) {
      cf.setSocketConfigurator(config.getSocketConfigurator());
    }
    if (config.getSocketFactory() != null) {
      cf.setSocketFactory(config.getSocketFactory());
    }
    if (config.getSslContextFactory() != null) {
      cf.setSslContextFactory(config.getSslContextFactory());
    }    
    if (config.getThreadFactory() != null) {
      cf.setThreadFactory(config.getThreadFactory());
    }    
    if (config.getTopologyRecoveryExecutor() != null) {
      cf.setTopologyRecoveryExecutor(config.getTopologyRecoveryExecutor());
    }    
    if (config.getTopologyRecoveryFilter() != null) {
      cf.setTopologyRecoveryFilter(config.getTopologyRecoveryFilter());
    }    
    if (config.getTopologyRecoveryRetryHandler() != null) {
      cf.setTopologyRecoveryRetryHandler(config.getTopologyRecoveryRetryHandler());
    }    
    if (config.getTrafficListener() != null) {
      cf.setTrafficListener(config.getTrafficListener());
    }    

    Connection conn = addresses == null
           ? cf.newConnection(config.getConnectionName())
           : cf.newConnection(addresses, config.getConnectionName());
    lastConnectedInstance = connectCount.incrementAndGet();
    connectionName = config.getConnectionName();
    conn.setId(Long.toString(lastConnectedInstance));
    logger.info("Established connection to amqp{}://{}@{}/{}"
            , cf.isSSL() ? "s" : ""
            , cf.getUsername()
            , conn.getAddress()
            , URLEncoder.encode(cf.getVirtualHost(), "UTF-8")
    );      
    conn.addShutdownListener(this);
    
    return conn;
  }

  private void configureSslProtocol(ConnectionFactory cf) {
    //The RabbitMQ Client connection needs a JDK SSLContext, so force this setting.
//    config.setSslEngineOptions(new JdkSSLEngineOptions());
//    config.getEnabledSecureTransportProtocols();
//    config.setSsl(true);
//    // config.addEnabledSecureTransportProtocol("TLSv1.3");
//    SSLHelper sslHelper = new SSLHelper(config, config.getKeyCertOptions(), config.getTrustOptions());
//    JdkSslContext ctx = (JdkSslContext) sslHelper.getContext((VertxInternal) vertx);
//    NioParams nioParams = config.getNioParams();
//    if (nioParams == null) {
//      nioParams = new NioParams();
//    }
//    nioParams.setSslEngineConfigurator(sslEngine -> {
//      sslEngine.setUseClientMode(true);
//    });
//    config.setNioParams(nioParams);
//    cf.useSslProtocol(ctx.context());
  }

  @Override
  public void shutdownCompleted(ShutdownSignalException cause) {
    logger.info("Connection {} Shutdown: {}", ((Connection) cause.getReference()).getId(), cause.getMessage());
  }
  
  protected boolean shouldRetryConnection() {
    if ((config.getReconnectInterval() > 0) 
            && ((config.getReconnectAttempts() < 0) || config.getReconnectAttempts() > reconnectCount)
            && (connectedAtLeastOnce || config.isReconnectOnInitialConnection())
            && !closed
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
        if (connection == null || !connection.isOpen()) {
          connection = rawConnect();
          connectedAtLeastOnce = true;
        }
        promise.complete(connection);
      }
    } catch(Throwable ex) {
      logger.error("Failed to create connection: ", ex);
      if (shouldRetryConnection()) {
        vertx.setTimer(config.getReconnectInterval(), time -> connectBlocking(promise));
      } else {
        promise.fail(ex);
      }
    }
  }  
  
  public Future<Channel> openChannel(long lastInstance) {
    synchronized(connectingPromiseLock) {
      if (((connectingFuture == null) || (lastInstance != this.connectCount.get())) && !closed) {
        synchronized(connectionLock) {       
          if (lastConnectedInstance > 0) {
            logger.info("Break");
          }
          if (lastConnectedInstance != connectCount.get()) {
            reconnectCount = 0;
          }
        }
        Promise<Connection> connectingPromise = Promise.promise();
        connectingFuture = connectingPromise.future();
        context.executeBlocking(execPromise -> connectBlocking(connectingPromise));
      }
      return connectingFuture
              .compose(conn -> {
                return context.executeBlocking(promise -> {
                  try {                    
                    promise.complete(conn.createChannel());
                  } catch(IOException ex) {
                    logger.error("Failed to create channel: ", ex);
                    if (shouldRetryConnection()) {
                      openChannel(lastInstance).onComplete(promise);
                    }
                    promise.fail(ex);
                  }
                });
              });
    }
  }

  @Override
  public RabbitMQChannel createChannel() {
    return new RabbitMQChannelImpl(vertx, this, config);
  }

  @Override
  public Future<Void> close(int closeCode, String closeMessage, int timeout) {
    Connection conn = connection;
    closed = true;
    if (conn == null) {
      return Future.succeededFuture();
    }
    return context.executeBlocking(promise -> {
      try {        
        conn.close(closeCode, closeMessage, timeout);
        promise.complete();
      } catch(Throwable ex) {
        promise.fail(ex);
      }
    });
  }

  @Override
  public Future<Void> close() {
    return close(AMQP.REPLY_SUCCESS, "OK", config.getHandshakeTimeout());
  }
  
}
