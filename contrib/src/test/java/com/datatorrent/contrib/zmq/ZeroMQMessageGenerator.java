package com.datatorrent.contrib.zmq;

import java.util.HashMap;

import org.slf4j.Logger;
import org.zeromq.ZMQ;

class ZeroMQMessageGenerator {
  private ZMQ.Context context;
  private ZMQ.Socket publisher;
  private ZMQ.Socket syncservice;
  private final int SUBSCRIBERS_EXPECTED = 1;

  String pubAddr = "tcp://*:5556";
  String syncAddr = "tcp://*:5557";

  private static Logger logger;
  public ZeroMQMessageGenerator(Logger loggerInstance)
  {
    logger = loggerInstance;
  }

  public void setup()
  {
    context = ZMQ.context(1);
    logger.debug("Publishing on ZeroMQ");
    publisher = context.socket(ZMQ.PUB);
    publisher.bind(pubAddr);
    syncservice = context.socket(ZMQ.REP);
    syncservice.bind(syncAddr);
  }

  public void send(Object message)
  {
    String msg = message.toString();
    // logger.debug("publish:"+msg);
    publisher.send(msg.getBytes(), 0);
  }

  public void teardown()
  {
    publisher.close();
    context.term();
  }

  public void generateMessages(int msgCount) throws InterruptedException
  {
    for (int subscribers = 0; subscribers < SUBSCRIBERS_EXPECTED; subscribers++) {
      byte[] value = syncservice.recv(0);
      syncservice.send("".getBytes(), 0);
    }
    for (int i = 0; i < msgCount; i++) {
      HashMap<String, Integer> dataMapa = new HashMap<String, Integer>();
      dataMapa.put("a", 2);
      send(dataMapa);

      HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
      dataMapb.put("b", 20);
      send(dataMapb);

      HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
      dataMapc.put("c", 1000);
      send(dataMapc);
    }
  }
}
