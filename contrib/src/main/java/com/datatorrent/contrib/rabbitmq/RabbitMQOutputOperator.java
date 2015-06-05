package com.datatorrent.contrib.rabbitmq;

import java.io.IOException;

public class RabbitMQOutputOperator extends AbstractSinglePortRabbitMQOutputOperator<byte[]>
{
  @Override
  public void processTuple(byte[] tuple)
  {
      try {
        channel.basicPublish(exchange, "", null, tuple);
      } catch (IOException e) {
        
        logger.debug(e.toString());
      }   
  }
}