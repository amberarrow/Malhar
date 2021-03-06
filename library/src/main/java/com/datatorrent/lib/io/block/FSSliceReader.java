/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.io.block;

import org.apache.hadoop.fs.FSDataInputStream;

import com.datatorrent.api.StatsListener;

import com.datatorrent.common.util.Slice;

/**
 * An {@link AbstractFSBlockReader} which emits fixed-size byte-arrays wrapped in {@link Slice}.<br/>
 */
@StatsListener.DataQueueSize
public class FSSliceReader extends AbstractFSBlockReader<Slice>
{
  public FSSliceReader()
  {
    super();
    this.readerContext = new ReaderContext.FixedBytesReaderContext<FSDataInputStream>();
  }

  @Override
  protected Slice convertToRecord(byte[] bytes)
  {
    return new Slice(bytes);
  }
}
