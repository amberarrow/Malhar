package com.datatorrent.demos.dimensions.generic;

import org.junit.Assert;
import org.junit.Test;

public class GenericDimensionComputationTest
{
  @Test
  public void test()
  {
    GenericDimensionComputation dc = new GenericDimensionComputation();
    dc.setEventSchemaJSON(GenericAggregateSerializerTest.TEST_SCHEMA_JSON);
    dc.setup(null);

    Assert.assertEquals("Total number of aggregators ", 8, dc.getAggregators().length);
  }
}
