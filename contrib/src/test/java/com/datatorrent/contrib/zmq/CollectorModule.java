package com.datatorrent.contrib.zmq;

import java.util.ArrayList;
import java.util.HashMap;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import java.util.List;

class CollectorInputPort<T> extends DefaultInputPort<T>
{
	public static HashMap<String, List<?>> collections = new HashMap<String, List<?>>();
	ArrayList<T> list;

	final String id;

	public CollectorInputPort(String id, Operator module)
	{
		super();
		this.id = id;
	}

  @Override
  public void process(T tuple)
  {
//    System.out.print("collector process:"+tuple);
    list.add(tuple);
  }

  @Override
  public void setConnected(boolean flag)
  {
    if (flag) {
      collections.put(id, list = new ArrayList<T>());
    }
  }
}

class CollectorModule<T> extends BaseOperator
{
  public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("collector", this);
}