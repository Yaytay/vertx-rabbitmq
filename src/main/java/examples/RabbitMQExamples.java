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
package examples;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Envelope;
import io.vertx.core.Vertx;
import io.vertx.rabbitmq.DefaultConsumer;
import io.vertx.rabbitmq.RabbitMQChannel;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQConnection;
import io.vertx.rabbitmq.RabbitMQOptions;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 *
 * @author jtalbut
 */
public class RabbitMQExamples {
  
  public void createConnectionWithUri() {
    Vertx vertx = Vertx.vertx();
    RabbitMQOptions config = new RabbitMQOptions();
    config.setUri("amqp://brokerhost/vhost");
    config.setConnectionName(this.getClass().getSimpleName());
    
    RabbitMQConnection connection = RabbitMQClient.create(vertx, config);    
    RabbitMQChannel channel = connection.createChannel();
    channel.connect()
            .onComplete(ar -> {
            });
  }
  
  public void createConnectionWithManualParameters() {
    Vertx vertx = Vertx.vertx();
    RabbitMQOptions config = new RabbitMQOptions();
    config.setHost("brokerhost");
    config.setPort(5672);
    config.setVirtualHost("vhost");
    config.setConnectionName(this.getClass().getSimpleName());
    
    RabbitMQConnection connection = RabbitMQClient.create(vertx, config);    
    RabbitMQChannel channel = connection.createChannel();
    channel.connect()
            .onComplete(ar -> {
            });
  }
  
  public void createConnectionWithMultipleHost() {
    Vertx vertx = Vertx.vertx();
    RabbitMQOptions config = new RabbitMQOptions();
    config.setAddresses(
            Arrays.asList(
                    Address.parseAddress("brokerhost1:5672")
                    , Address.parseAddress("brokerhost2:5672")
            )
    );
    config.setVirtualHost("vhost");
    config.setConnectionName(this.getClass().getSimpleName());
    
    RabbitMQConnection connection = RabbitMQClient.create(vertx, config);    
    RabbitMQChannel channel = connection.createChannel();
    channel.connect()
            .onComplete(ar -> {
            });
  }
  
  public void createConnectionAndUseImmediately() {
    Vertx vertx = Vertx.vertx();
    RabbitMQOptions config = new RabbitMQOptions();
    config.setUri("amqp://brokerhost/vhost");
    config.setConnectionName(this.getClass().getSimpleName());
    
    RabbitMQConnection connection = RabbitMQClient.create(vertx, config);    
    RabbitMQChannel channel = connection.createChannel();
    channel.exchangeDeclare("exchange", BuiltinExchangeType.FANOUT, true, true, null)
            .compose(v -> channel.queueDeclare("queue", true, true, true, null))
            .compose(v -> channel.queueBind("queue", "exchange", "", null))
            .onComplete(ar -> {
            });
  }
  
  public void basicPublish() {
    Vertx vertx = Vertx.vertx();
    RabbitMQOptions config = new RabbitMQOptions();
    config.setUri("amqp://brokerhost/vhost");
    config.setConnectionName(this.getClass().getSimpleName());
    
    RabbitMQConnection connection = RabbitMQClient.create(vertx, config);    
    RabbitMQChannel channel = connection.createChannel();
    channel.exchangeDeclare("exchange", BuiltinExchangeType.FANOUT, true, true, null)
            .compose(v -> channel.basicPublish("exchange", "routingKey", false, null, "Body".getBytes(StandardCharsets.UTF_8)))
            .onComplete(ar -> {
            });
  }
  
  private static class ExampleConsumer extends DefaultConsumer {

    public ExampleConsumer(RabbitMQChannel channel) {
      super(channel);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
      System.out.println(new String(body, StandardCharsets.UTF_8));
    }
    
  }
  
  public void basicConsume() {
    Vertx vertx = Vertx.vertx();
    RabbitMQOptions config = new RabbitMQOptions();
    config.setUri("amqp://brokerhost/vhost");
    config.setConnectionName(this.getClass().getSimpleName());
    
    RabbitMQConnection connection = RabbitMQClient.create(vertx, config);    
    RabbitMQChannel channel = connection.createChannel();
    channel.exchangeDeclare("exchange", BuiltinExchangeType.FANOUT, true, true, null)
            .compose(v -> channel.queueDeclare("queue", true, true, true, null))
            .compose(v -> channel.queueBind("queue", "exchange", "", null))
            .compose(v -> channel.basicConsume("queue", true, channel.getChannelId(), false, false, null, new ExampleConsumer(channel)))
            .onComplete(ar -> {
            });
  }
  
}
