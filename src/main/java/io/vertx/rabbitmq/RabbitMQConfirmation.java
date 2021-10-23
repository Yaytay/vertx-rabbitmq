package io.vertx.rabbitmq;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

/**
 *
 * @author jtalbut
 */
@DataObject(generateConverter = true)
public class RabbitMQConfirmation {
  
  private String channelId;
  private long deliveryTag;
  private boolean multiple;
  private boolean succeeded;

  public RabbitMQConfirmation(String channelId, long deliveryTag, boolean multiple, boolean succeeded) {
    this.channelId = channelId;
    this.deliveryTag = deliveryTag;
    this.multiple = multiple;
    this.succeeded = succeeded;
  }
  
  public RabbitMQConfirmation(JsonObject json) {
    RabbitMQConfirmationConverter.fromJson(json, this);
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    RabbitMQConfirmationConverter.toJson(this, json);
    return json;
  }

  public String getChannelId() {
    return channelId;
  }

  public long getDeliveryTag() {
    return deliveryTag;
  }

  public boolean isMultiple() {
    return multiple;
  }

  public boolean isSucceeded() {
    return succeeded;
  }

}
