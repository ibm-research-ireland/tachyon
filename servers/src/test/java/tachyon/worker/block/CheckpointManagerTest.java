/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block;

import java.io.ByteArrayOutputStream;
import java.io.IOException; import java.io.OutputStream; import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import tachyon.Constants;
import tachyon.Users;
import tachyon.underfs.UnderFileSystem;
import tachyon.worker.block.io.BlockReader;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.thrift.ClientFileInfo;

public final class CheckpointManagerTest  {

  private static final int FAKE_BLOCK_SIZE_BYTES = 35 * Constants.KB;
  private static final List<Long> FAKE_BLOCK_IDS = 
      com.google.common.collect.ImmutableList.of(1L,2L,3L);
  
  private static final byte[] EXPECT_FILE_CONTENT;
  
  static {
    EXPECT_FILE_CONTENT = new byte[CheckpointManagerTest.FAKE_BLOCK_SIZE_BYTES
                                   * CheckpointManagerTest.FAKE_BLOCK_IDS.size()];
    
    for (int i = 0; i < FAKE_BLOCK_IDS.size(); i++) {
      final int start = i * FAKE_BLOCK_SIZE_BYTES;
      final int end = start + FAKE_BLOCK_SIZE_BYTES;
      Arrays.fill(EXPECT_FILE_CONTENT, start, end, (byte) (long) FAKE_BLOCK_IDS.get(i));
    }

  }


  // what should I test, exactly to test the manager in isolation? 
  private BlockDataManager mBlockDataManagerMock;
  private BlockStore mBlockStoreMock;
  private UnderFileSystem mUfsMock;
  private List<ByteArrayOutputStream> mMockOutputs;

  private CheckpointManager mUnderTest;

  @Before
  public void setUp() throws IOException {
    mMockOutputs = new ArrayList<ByteArrayOutputStream>();
    mBlockDataManagerMock = mockBlockDataManager(); 
    mBlockStoreMock = mockBlockStore();
    mUfsMock = mockUfs();

    mUnderTest = new CheckpointManager(mBlockDataManagerMock, 
        mBlockStoreMock,
        mUfsMock,
        2,
        10000);

  }

  @After 
  public void tearDown() throws InterruptedException {
    mUnderTest.stop();
  }


  @Test
  public void testAsyncCheckpoint() throws InterruptedException, TException, IOException {
    final int howmanyfiles = 5;
    final ClientFileInfo[] fileinfos =  new ClientFileInfo[howmanyfiles];
    for (int i = 0; i < howmanyfiles; i++) {
      fileinfos[i] = generateMockFileInfo(i);
    }
    mUnderTest.start();

    for (int i = 0; i < howmanyfiles; i++) {
      mUnderTest.scheduleForCheckpoint(0, fileinfos[i]);
    }
    Thread.sleep(3000);

    for (int i = 0; i < howmanyfiles; i++) {
      Mockito.verify(mBlockDataManagerMock).addCheckpoint(Users.CHECKPOINT_USER_ID, i);
      Mockito.verify(mUfsMock).create("/data/" + i, CheckpointManagerTest.FAKE_BLOCK_SIZE_BYTES);
      synchronized (mMockOutputs) {
        final ByteArrayOutputStream os = mMockOutputs.get(i);
        final byte[] out = os.toByteArray();
        Assert.assertArrayEquals(EXPECT_FILE_CONTENT, out);
      }
    }
  }
  
  @Test
  public void testSyncCheckpoint() throws InterruptedException, TException, IOException {
    final int howmanyfiles = 5;
    final ClientFileInfo[] fileinfos =  new ClientFileInfo[howmanyfiles];
    for (int i = 0; i < howmanyfiles; i++) {
      fileinfos[i] = generateMockFileInfo(i);
    }

    for (int i = 0; i < howmanyfiles; i++) {
      // Note that the CheckpointingManager is not started
      // so async checkpoint won't take place
      mUnderTest.scheduleForCheckpoint(Users.CHECKPOINT_USER_ID, fileinfos[i]);
    }
    Thread.sleep(3000);
    Mockito.verifyZeroInteractions(mUfsMock);

    for (int i = 0; i < howmanyfiles; i++) {
      // forces checkpoint synchronously
      mUnderTest.ensureCheckpointed(i, 0);
      Mockito.verify(mBlockDataManagerMock).addCheckpoint(0, i);
      Mockito.verify(mUfsMock).create("/data/" + i, CheckpointManagerTest.FAKE_BLOCK_SIZE_BYTES);
      synchronized (mMockOutputs) {
        final ByteArrayOutputStream os = mMockOutputs.get(i);
        final byte[] out = os.toByteArray();
        Assert.assertArrayEquals(EXPECT_FILE_CONTENT, out);
      }
    }
  }
  
  @Test 
  public void testMixedCheckpoint() throws InterruptedException, TException, IOException {
    final int howmanyfiles = 5;
    final ClientFileInfo[] fileinfos =  new ClientFileInfo[howmanyfiles];
    for (int i = 0; i < howmanyfiles; i++) {
      fileinfos[i] = generateMockFileInfo(i);
    }

    mUnderTest.start();
    for (int i = 0; i < howmanyfiles; i++) {
      mUnderTest.scheduleForCheckpoint(Users.CHECKPOINT_USER_ID, fileinfos[i]);
    }

    for (int i = 0; i < howmanyfiles; i++) {
      // forces checkpoint synchronously
      // this will proceed concunrrently to the async checkpoint
      mUnderTest.ensureCheckpointed(i, Users.CHECKPOINT_USER_ID);
    }
    
    Thread.sleep(3000);
    for (int i = 0; i < howmanyfiles; i++) {
      Mockito.verify(mBlockDataManagerMock).addCheckpoint(Users.CHECKPOINT_USER_ID, i);
      Mockito.verify(mUfsMock).create("/data/" + i, CheckpointManagerTest.FAKE_BLOCK_SIZE_BYTES);
      synchronized (mMockOutputs) {
        final ByteArrayOutputStream os = mMockOutputs.get(i);
        final byte[] out = os.toByteArray();
        Assert.assertArrayEquals(EXPECT_FILE_CONTENT, out);
      }

    }
  } 



  private static ClientFileInfo generateMockFileInfo(int fileId)  {
    ClientFileInfo ret = new ClientFileInfo();

    ret.id = fileId;
    ret.name = "MockFile";
    ret.path = "/mock/path";
    ret.ufsPath = "";
    ret.length = CheckpointManagerTest.FAKE_BLOCK_SIZE_BYTES 
      * CheckpointManagerTest.FAKE_BLOCK_IDS.size() ;
    ret.blockSizeByte = CheckpointManagerTest.FAKE_BLOCK_SIZE_BYTES;
    ret.creationTimeMs = 0L;
    ret.isComplete = true;
    ret.isFolder = false;
    ret.isPinned = false;
    ret.isCache = true;
    ret.blockIds = CheckpointManagerTest.FAKE_BLOCK_IDS; 
    ret.dependencyId = -1;
    ret.inMemoryPercentage = 100;
    ret.lastModificationTimeMs = 0L;

    return ret;
  }


  private UnderFileSystem mockUfs() throws IOException {
    UnderFileSystem ufs  = Mockito.mock(UnderFileSystem.class);
    Mockito.when(ufs.create(Mockito.anyString(), Mockito.anyInt()))
        .thenAnswer(new Answer<OutputStream>() {
          @Override
          public OutputStream answer(InvocationOnMock invocation) 
              throws Throwable {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            synchronized (mMockOutputs) {
              mMockOutputs.add(os);
            }
            return os;
          }
        });
    return ufs;
  }



  private BlockDataManager mockBlockDataManager() throws IOException {
    BlockDataManager mock = Mockito.mock(BlockDataManager.class);
    Mockito.when(mock.getUserUfsTmpFolder(Mockito.anyLong())).thenReturn("/data/");
    return mock;
  }

  private BlockStore mockBlockStore() throws IOException {
    final BlockStore mock = Mockito.mock(BlockStore.class);

    Mockito.when(mock.lockBlock(Mockito.anyLong(), Mockito.anyLong()))
      .thenReturn(1L);

    Mockito.when(mock.getBlockReader(Mockito.anyLong(), 
          Mockito.anyLong(), Mockito.anyLong()))
        .thenAnswer(new Answer<BlockReader>() {
          public BlockReader answer(InvocationOnMock invocation) throws IOException {
            Object[] args = invocation.getArguments();
            long blockId = (Long) args[1];
            return generateBlockReader(blockId);
          }

        });

    Mockito.when(mock.getBlockMeta(Mockito.anyLong(), Mockito.anyLong(),
          Mockito.anyLong())).thenReturn(new BlockMeta(0, 0, 
          Mockito.mock(StorageDir.class)));

    return mock;
  }



  private BlockReader generateBlockReader(long blockId) throws IOException {
    final BlockReader mock = Mockito.mock(BlockReader.class);
    final byte[] buff = new byte[CheckpointManagerTest.FAKE_BLOCK_SIZE_BYTES];
    // fill with the less significant bits of the block id
    // This is just a convention to know what to expect in the 
    // array
    Arrays.fill(buff,(byte) blockId);
    final ByteBuffer bb = ByteBuffer.wrap(buff);

    Mockito.when(mock.read(Mockito.anyLong(), Mockito.anyLong()))
      .thenReturn(bb);

    return mock;

  }
  


}
