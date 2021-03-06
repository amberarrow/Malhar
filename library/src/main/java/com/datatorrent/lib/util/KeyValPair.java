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
package com.datatorrent.lib.util;

import java.util.AbstractMap;

/**
 *
 * A single KeyValPair for basic data passing, It is a write once, and read often model. <p>
 * <br>
 * Key and Value are to be treated as immutable objects.
 *
 * @param <K>
 * @param <V>
 * @since 0.3.2
 */
public class KeyValPair<K, V> extends AbstractMap.SimpleEntry<K, V>
{
  private static final long serialVersionUID = 201301281547L;

  /**
   * Added default constructor for deserializer.
   */
  private KeyValPair()
  {
    super(null, null);
  }

  /**
   * Constructor
   *
   * @param k sets key
   * @param v sets value
   */
  public KeyValPair(K k, V v)
  {
    super(k, v);
  }

}
