package com.datatorrent.contrib.testhelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;


public class CollectorModule<T> extends BaseOperator
{
  public final transient CollectorInputPort<T> inputPort = new CollectorInputPort<T>("collector", this);
}
