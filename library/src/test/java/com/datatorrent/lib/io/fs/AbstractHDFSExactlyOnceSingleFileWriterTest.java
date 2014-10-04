/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.io.fs;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Rule;
import org.junit.Test;

public class AbstractHDFSExactlyOnceSingleFileWriterTest
{
  private static final String SINGLE_FILE = "single.txt";

  @Rule public IOTestHelper testMeta = new IOTestHelper();

  /**
   * Dummy writer to store checkpointed state
   */
  private static class CheckPointWriter extends AbstractHDFSExactlyOnceSingleFileWriter<Integer>
  {
    @Override
    protected byte[] getBytesForTuple(Integer tuple)
    {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }

  /**
   * Simple writer which writes to one file.
   */
  private static class SingleHDFSExactlyOnceWriter extends AbstractHDFSExactlyOnceSingleFileWriter<Integer>
  {
    @Override
    protected FileSystem getFSInstance() throws IOException
    {
      return FileSystem.getLocal(new Configuration()).getRaw();
    }

    @Override
    protected byte[] getBytesForTuple(Integer tuple)
    {
      return (tuple.toString() + "\n").getBytes();
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private CheckPointWriter checkpoint(AbstractHDFSExactlyOnceSingleFileWriter<Integer> writer)
  {
    CheckPointWriter checkPointWriter = new CheckPointWriter();
    checkPointWriter.append = writer.append;
    checkPointWriter.counts = Maps.newHashMap();

    for(String keys: writer.counts.keySet()) {
      checkPointWriter.counts.put(keys,
                                  new MutableLong(writer.counts.get(keys).longValue()));
    }

    checkPointWriter.endOffsets = Maps.newHashMap();

    for(String keys: writer.endOffsets.keySet()) {
      checkPointWriter.endOffsets.put(keys, new MutableLong(writer.endOffsets.get(keys).longValue()));
    }

    checkPointWriter.openPart = Maps.newHashMap();

    for(String keys: writer.openPart.keySet()) {
      checkPointWriter.openPart.put(keys,
                                    new MutableInt(writer.openPart.get(keys).intValue()));
    }

    checkPointWriter.filePath = writer.filePath;
    checkPointWriter.maxOpenFiles = writer.maxOpenFiles;
    checkPointWriter.replication = writer.replication;
    checkPointWriter.totalBytesWritten = writer.totalBytesWritten;
    checkPointWriter.maxLength = writer.maxLength;
    checkPointWriter.rollingFile = writer.rollingFile;
    checkPointWriter.outputFileName = writer.outputFileName;

    return checkPointWriter;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void restoreCheckPoint(CheckPointWriter checkPointWriter,
                                 AbstractHDFSExactlyOnceSingleFileWriter<Integer> writer)
  {
    writer.append = checkPointWriter.append;
    writer.counts = checkPointWriter.counts;
    writer.endOffsets = checkPointWriter.endOffsets;
    writer.openPart = checkPointWriter.openPart;
    writer.filePath = checkPointWriter.filePath;
    writer.maxOpenFiles = checkPointWriter.maxOpenFiles;
    writer.replication = checkPointWriter.replication;
    writer.totalBytesWritten = checkPointWriter.totalBytesWritten;
    writer.maxLength = checkPointWriter.maxLength;
    writer.rollingFile = checkPointWriter.rollingFile;
    writer.outputFileName = checkPointWriter.outputFileName;
  }

  private void prepareTest()
  {
    File testDir = new File(testMeta.dir);
    FileUtils.deleteQuietly(testDir);

    testDir.mkdir();
  }

  @Test
  public void testSingleFileCompletedWrite()
  {
    prepareTest();
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);
    writer.setOutputFileName(SINGLE_FILE);

    testSingleFileCompletedWriteHelper(writer);
  }

  @Test
  public void testSingleFileCompletedWriteOverwrite()
  {
    prepareTest();
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(false);
    writer.setOutputFileName(SINGLE_FILE);

    testSingleFileCompletedWriteHelper(writer);
  }

  private void testSingleFileCompletedWriteHelper(SingleHDFSExactlyOnceWriter writer)
  {
    writer.setFilePath(testMeta.dir);

    writer.setup(new DummyContext(0));

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.endWindow();

    writer.beginWindow(1);
    writer.input.put(2);
    writer.input.put(3);
    writer.endWindow();

    writer.teardown();

    String singleFileName = testMeta.dir + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "2\n" +
                             "3\n";

    AbstractHDFSExactlyOnceWriterTest.checkOutput(-1,
                                      singleFileName,
                                      correctContents);
  }

  @Test
  public void testSingleFileFailedWrite()
  {
    prepareTest();
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(true);
    writer.setOutputFileName(SINGLE_FILE);

    testSingleFileFailedWriteHelper(writer);

    String singleFileName = testMeta.dir + File.separator + SINGLE_FILE;

    String correctContents = "0\n" +
                             "1\n" +
                             "4\n" +
                             "5\n" +
                             "6\n" +
                             "7\n";

    AbstractHDFSExactlyOnceWriterTest.checkOutput(-1,
                                      singleFileName,
                                      correctContents);
  }

  @Test
  public void testSingleFileFailedWriteOverwrite()
  {
    prepareTest();
    SingleHDFSExactlyOnceWriter writer = new SingleHDFSExactlyOnceWriter();
    writer.setAppend(false);
    writer.setOutputFileName(SINGLE_FILE);

    testSingleFileFailedWriteHelper(writer);

    String singleFileName = testMeta.dir + File.separator + SINGLE_FILE;

    String correctContents = "4\n" +
                             "5\n" +
                             "6\n" +
                             "7\n";

    AbstractHDFSExactlyOnceWriterTest.checkOutput(-1,
                                                  singleFileName,
                                                  correctContents);
  }

  private void testSingleFileFailedWriteHelper(SingleHDFSExactlyOnceWriter writer)
  {
    File meta = new File(testMeta.dir);
    writer.setFilePath(meta.getAbsolutePath());

    writer.setup(new DummyContext(0));

    writer.beginWindow(0);
    writer.input.put(0);
    writer.input.put(1);
    writer.endWindow();

    CheckPointWriter checkPointWriter = checkpoint(writer);

    writer.beginWindow(1);
    writer.input.put(2);

    writer.teardown();

    restoreCheckPoint(checkPointWriter,
                      writer);
    writer.setup(new DummyContext(0));

    writer.beginWindow(1);
    writer.input.put(4);
    writer.input.put(5);
    writer.endWindow();

    writer.beginWindow(2);
    writer.input.put(6);
    writer.input.put(7);
    writer.endWindow();

    writer.teardown();
  }
}
