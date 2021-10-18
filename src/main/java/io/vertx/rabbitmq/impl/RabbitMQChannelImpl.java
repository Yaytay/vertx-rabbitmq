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
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.rabbitmq.RabbitMQChannel;
import io.vertx.rabbitmq.RabbitMQConsumer;
import io.vertx.rabbitmq.RabbitMQPublisher;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jtalbut
 */
public class RabbitMQChannelImpl implements RabbitMQChannel, ShutdownListener {
  
  @SuppressWarnings("constantname")
  private static final Logger logger = LoggerFactory.getLogger(RabbitMQChannelImpl.class);
  
  private final Vertx vertx;
  private final RabbitMQConnectionImpl connection;
  
  private volatile Channel channel;
  
  private final List<Handler<Promise<Void>>> channelEstablishedCallbacks = new ArrayList<>();

  public RabbitMQChannelImpl(Vertx vertx, RabbitMQConnectionImpl connection) {
    this.vertx = vertx;
    this.connection = connection;
  }

  @Override
  public void addChannelEstablishedCallback(Handler<Promise<Void>> channelEstablishedCallback) {
    synchronized(channelEstablishedCallbacks) {
      channelEstablishedCallbacks.add(channelEstablishedCallback);
    }
  }

  @Override
  public RabbitMQPublisher publish(String exchange) {
    return new RabbitMQPublisherImpl(vertx, this);
  }

  @Override
  public RabbitMQConsumer consumer(String queue) {
    return new RabbitMQConsumerImpl(vertx, this);
  }
    
  private void channelCallbackHandler(AsyncResult<Void> prevResult, Iterator<Handler<Promise<Void>>> iter, Promise<Void> connectPromise) {
    try {
      if (prevResult.failed()) {
        connectPromise.fail(prevResult.cause());
      } else {
        if (iter.hasNext()) {
          Handler<Promise<Void>> next = iter.next();
          Promise<Void> callbackPromise = Promise.promise();
          next.handle(callbackPromise);
          callbackPromise.future().onComplete(result -> channelCallbackHandler(result, iter, connectPromise));
        } else {
          connectPromise.complete();
        }
      }
    } catch (Throwable ex) {
      logger.error("Exception whilst running channel established callback: ", ex);
      connectPromise.fail(ex);
    }
  }
  
  @Override
  public Future<Void> connect() {
    
    Promise<Void> result = Promise.promise();
    connection.openChannel()
            .onComplete(ar -> {
                    if (ar.failed()) {
                      result.fail(ar.cause());
                    } else {
                      Channel chann = ar.result();
                      this.channel = chann;
                      chann.addShutdownListener(this);

                      List<Handler<Promise<Void>>> callbacks;
                      synchronized(channelEstablishedCallbacks) {
                        callbacks = new ArrayList<>(channelEstablishedCallbacks);
                      } 
                      if (callbacks.isEmpty()) {
                        result.complete();
                      } else {
                        channelCallbackHandler(Future.succeededFuture(), callbacks.iterator(), result);
                      }
                    }
            });
   
    return result.future();
  }

  @Override
  public void shutdownCompleted(ShutdownSignalException cause) {
    logger.info("Channel Shutdown: {}", cause.getMessage());
  }
    
  private interface ChannelHandler<T> {
    T handle() throws Exception;
  }
  
  private <T> Future<T> onChannel(ChannelHandler<T> handler) {
    ContextInternal ctx = (ContextInternal) vertx.getOrCreateContext();
    if (channel == null || !channel.isOpen()) {
      return connect().compose(v -> onChannel(handler));
    }
    return vertx.executeBlocking(future -> {
      try {
        T t = handler.handle();
        future.complete(t);
      } catch (Throwable t) {
        future.fail(t);
      }
    });
  }
  
  @Override
  public Future<Void> abort(int closeCode, String closeMessage) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Future<Void> addConfirmListener(ConfirmCallback ackCallback, ConfirmCallback nackCallback) {
    
    return onChannel(() -> {
      return channel.addConfirmListener(ackCallback, nackCallback);
    }).mapEmpty();
    
  }
  
  @Override
  public Future<Void> basicAck(long deliveryTag, boolean multiple) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Future<Void> basicConsume(String queue, Consumer consumer) {
    
    return onChannel(() -> {
      return channel.basicConsume(queue, consumer);
    }).mapEmpty();
    
  }

  @Override
  public Future<Void> basicPublish(String exchange, String routingKey, boolean mandatory, AMQP.BasicProperties props, byte[] body) {
    
    /**
     * This is an optimisation that avoids considering basicPublish to be a blocking operation as it translates directly to an NIO call.
     * This is only valid if:
     * 1. The RabbitMQClient is using NIO.
     * 2. Synchronous confirms are not enabled.
     */
    try {
      if (channel != null && channel.isOpen()) {
        channel.basicPublish(exchange, routingKey, mandatory, props, body);
        return Future.succeededFuture();
      }
    } catch(IOException ex) {
      logger.warn("Synchronous send of basicPublish({}, {}, {}, ...) failed: ", exchange, routingKey, mandatory, ex);
    }
    
    return onChannel(() -> {
      channel.basicPublish(exchange, routingKey, mandatory, props, body);
      return null;
    }).mapEmpty();
    
  }

  @Override
  public Future<Void> basicQos(int prefetchSize, int prefetchCount, boolean global) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Future<Void> exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, Map<String, Object> arguments) {
    
    return onChannel(() -> {
      return channel.exchangeDeclare(exchange, type, durable, autoDelete, arguments);
    }).mapEmpty();
    
  }

  @Override
  public Future<Void> exchangeBind(String destination, String source, String routingKey, Map<String, Object> arguments) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Future<Void> queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
    
    return onChannel(() -> {
      return channel.queueDeclare(queue, durable, exclusive, autoDelete, arguments);
    }).mapEmpty();
    
  }

  @Override
  public Future<Void> queueDeclarePassive(String queue) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Future<Void> queueBind(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
    
    return onChannel(() -> {
      return channel.queueBind(queue, exchange, routingKey, arguments);
    }).mapEmpty();
    
  }

  @Override
  public void close() throws Exception {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  
  
}
