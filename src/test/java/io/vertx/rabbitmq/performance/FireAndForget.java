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
package io.vertx.rabbitmq.performance;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import io.vertx.core.Future;
import io.vertx.rabbitmq.RabbitMQChannel;
import io.vertx.rabbitmq.RabbitMQConnection;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jtalbut
 */
public class FireAndForget implements RabbitMQPublisherStresser {

  private final RabbitMQChannel channel;
  private final AtomicLong counter = new AtomicLong();
  private String exchange;

  public FireAndForget(RabbitMQConnection connection) {
    this.channel = connection.createChannel();
  }
  
  @Override
  public String getName() {
    return "Fire and forget (no message confirmation)";
  }

  @Override
  public Future<Void> init(String exchange) {
    this.exchange = exchange;
    return channel.exchangeDeclare(exchange, BuiltinExchangeType.FANOUT, true, false, null)
            ;
  }

  @Override
  public Future<Void> runTest(long iterations) {
    counter.set(iterations);
    long iter;
    while((iter = counter.decrementAndGet()) > 0) {
      Future pubFuture = channel.basicPublish(exchange, "", true, new AMQP.BasicProperties(), Long.toString(iter).getBytes());
      if (pubFuture.failed()) {
        return pubFuture;
      }
    }
    return Future.succeededFuture();
  }
  
}
