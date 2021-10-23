package io.vertx.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * A reliable publisher that
 * <ul>
 * <li>Queues up messages internally until it can successfully call basicPublish.
 * <li>Notifies the caller using a robust ID (not delivery tag) when the message is confirmed by rabbit.
 * </ul>
 * 
 * This is a layer above the RabbitMQClient that provides a lot of standard implementation when guaranteed at least once delivery is required.
 * If confirmations are not required do not use this publisher as it does have overhead.
 * 
 * @author jtalbut
 */
@VertxGen
public interface RabbitMQPublisher extends AutoCloseable { 
  
  /**
   * Stop the rabbitMQ publisher.
   * Calling this is optional, but it gives the opportunity to drain the send queue without losing messages.
   * Future calls to publish will be ignored.
   *
   */
  void stop(Handler<AsyncResult<Void>> resultHandler);
  
  /**
   * Like {@link #stop(Handler)} but returns a {@code Future} of the asynchronous result
   */
  Future<Void> stop();
  
  /**
   * Undo the effects of calling {@link #stop(Handler)} so that publish may be called again. 
   * It is harmless to call restart() when {@link #stop(Handler)} has not been called, however if restart() is called 
   * whilst {@link #stop(Handler)} is being processed the {@link #stop(Handler)} will never complete.
   * 
   */
  void restart();
  
  /**
   * Get the ReadStream that contains the message IDs for confirmed messages.
   * The message IDs in this ReadStream are taken from the message properties,
   * if these message IDs are not set then this ReadStream will contain nulls and using this publisher will be pointless.
   * 
   * @return the ReadStream that contains the message IDs for confirmed messages.
   */
  ReadStream<RabbitMQPublisherConfirmation> getConfirmationStream();

  /**
   * Publish a message. 
   * 
   * @see com.rabbitmq.client.Channel#basicPublish(String, String, AMQP.BasicProperties, byte[])
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  void publish(String routingKey, BasicProperties properties, Buffer body);

  /**
   * Publish a message. 
   * 
   * @see com.rabbitmq.client.Channel#basicPublish(String, String, AMQP.BasicProperties, byte[])
   */
  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  void publish(String routingKey, BasicProperties properties, byte[] body);

  /**
   * Get the number of published, but not sent, messages.
   * @return the number of published, but not sent, messages.
   */
  int queueSize();
  
}
