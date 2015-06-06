package com.datatorrent.contrib.testhelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Assert;

public class MessageQueueTestHelper {

  public static void validateResults(int testNum, HashMap<String, List<?>> collections )
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
  
  public static ArrayList<HashMap<String, Integer>> getMessages()  
  {
    ArrayList<HashMap<String, Integer>> mapList = new ArrayList<HashMap<String, Integer>>();
    
    HashMap<String, Integer> dataMapa = new HashMap<String, Integer>();
    dataMapa.put("a", 2);
    mapList.add(dataMapa);

    HashMap<String, Integer> dataMapb = new HashMap<String, Integer>();
    dataMapb.put("b", 20);
    mapList.add(dataMapb);

    HashMap<String, Integer> dataMapc = new HashMap<String, Integer>();
    dataMapc.put("c", 1000);
    mapList.add(dataMapc);
    
    return mapList;
    
  }
}
