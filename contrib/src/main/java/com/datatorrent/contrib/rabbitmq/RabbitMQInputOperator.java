package com.datatorrent.contrib.rabbitmq;

public class RabbitMQInputOperator extends AbstractSinglePortRabbitMQInputOperator<byte[]>
{
  @Override
  public byte[] getTuple(byte[] message)
  {
    return message;
  }
}
