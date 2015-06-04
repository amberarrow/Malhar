/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.zmq;

import com.datatorrent.contrib.zmq.AbstractSinglePortZeroMQInputOperator;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;
import com.datatorrent.api.DAG.Locality;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ZeroMQInputOperatorTest
{
  private static Logger logger = LoggerFactory.getLogger(ZeroMQInputOperatorTest.class);
  
  @Test
  public void testDag() throws InterruptedException, Exception {
	  final int testNum = 3;
	  testHelper(testNum);
  }

  protected void testHelper(final int testNum)
  {
	  LocalMode lma = LocalMode.newInstance();
	  DAG dag = lma.getDAG();

	  final ZeroMQMessageGenerator publisher = new ZeroMQMessageGenerator(logger);
	  publisher.setup();

	  ZeroMQInputOperator generator = dag.addOperator("Generator", ZeroMQInputOperator.class);
	  final CollectorModule<byte[]> collector = dag.addOperator("Collector", new CollectorModule<byte[]>());

	  generator.setFilter("");
	  generator.setUrl("tcp://localhost:5556");
	  generator.setSyncUrl("tcp://localhost:5557");

	  dag.addStream("Stream", generator.outputPort, collector.inputPort).setLocality(Locality.CONTAINER_LOCAL);
	  new Thread() {
		  @Override
		  public void run() {
			  try {
				  publisher.generateMessages(testNum);
			  }
			  catch (InterruptedException ex) {
				  logger.debug(ex.toString());
			  }
		  }
	  }.start();
	  
	  final LocalMode.Controller lc = lma.getController();
	  lc.setHeartbeatMonitoringEnabled(false);
	  
	  new Thread("LocalClusterController")
	  {
		  @Override
		  public void run()
		  {
			  int count = 0;
			  try {
				  Thread.sleep(1000);
				  while (true) {
					  if (count++ < testNum * 3) {
						  Thread.sleep(100);
					  }
					  else {
						  break;
					  }			            
				  }
			  }
			  catch (InterruptedException ex) {
			  }

			  logger.debug("Shutting down..");
			  lc.shutdown();

			  try {
				  Thread.sleep(1000);
			  } catch (InterruptedException e) {
				  // TODO Auto-generated catch block
				  e.printStackTrace();
			  }
			  
			  publisher.teardown();
			 
		  }
	  }.start();
	   
	  lc.run();

	  
	 // logger.debug("collection size:"+collector.inputPort.collections.size()+" "+collector.inputPort.collections.toString());

	  validateResults(testNum, collector.inputPort.collections);
	  logger.debug("end of test");
  }
  public void validateResults(int testNum, HashMap<String, List<?>> collections )
  {
	    ArrayList<byte[]> byteList =(ArrayList<byte[]>) collections.get("collector");
	    Assert.assertEquals("emitted value for testNum was ", testNum * 3, byteList.size());
	    for (int i = 0; i < byteList.size(); i++) {
	      String str = new String(byteList.get(i));
	      int eq = str.indexOf('=');
	      String key = str.substring(1, eq);
	      Integer value = Integer.parseInt(str.substring(eq + 1, str.length() - 1));
	      if (key.equals("a")) {
	        Assert.assertEquals("emitted value for 'a' was ", new Integer(2), value);
	      }
	      else if (key.equals("b")) {
	        Assert.assertEquals("emitted value for 'b' was ", new Integer(20), value);
	      }
	      if (key.equals("c")) {
	        Assert.assertEquals("emitted value for 'c' was ", new Integer(1000), value);
	      }
	    }
  }
}
