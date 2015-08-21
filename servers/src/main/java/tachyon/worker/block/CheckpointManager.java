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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.underfs.UnderFileSystem;
import tachyon.Users;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.util.CommonUtils;
import tachyon.util.ThreadFactoryUtils;
import tachyon.worker.block.io.BlockReader;
import tachyon.worker.block.meta.BlockMeta;


/**
 * Takes care of sync and async data checkpoints.
 *
 * This class exposes a minimal interface and it is intended to be used by tachyon.WorkerStorag
 * to handle checkpoints/write-backs of data written asynchrnously in memory. 
 * Differently from the original tachyon approach (at least in the latest 0.6 development branch)
 * this class ensures that no block is ever removed from memory if it was not written back
 * to the file system. This can be limiting, but it guarantees consistency. Maybe one day
 * it would be nice to add some configuration option.
 *
 * @author Andrea Reale <realean2@ie.ibm.com>
 */
// Many parts of this class are factored out from tachyon.WorkerStorage, 
// which was doing way too many things
public final class CheckpointManager {
  private static final byte MAX_CHECKPOINT_RETRIES = 3;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);


  private final int mNumCheckpointThreads;
  private final int mCheckpointCapacityPerSec;

  private static final class UncheckpointedFilesMapEntry {

    public final ClientFileInfo mFileInfo;
    public byte mRetries = 0; 

    public UncheckpointedFilesMapEntry(ClientFileInfo fi) {
      mFileInfo = fi;
    }
  }


  private final BlockDataManager mBlockDataManager;
  private final BlockStore mBlockStore;
  private final ScheduledExecutorService mCheckpointExecutor;
  private final UnderFileSystem mUfs;


  private volatile boolean mStarted = false;

  // guards the private data structures from concurrent access
  private final ReentrantLock mLock = new ReentrantLock();

  // list of files scheduled to be checkpointed (fileId -> numRetries)
  private final HashMap<Integer, UncheckpointedFilesMapEntry> mUncheckpointedFiles = 
      new HashMap<Integer, UncheckpointedFilesMapEntry>();
  private final LinkedList<Integer> mFifoUncheckpointedFiles =
      new LinkedList<Integer>();


  // list of files in the progress of being checkpointed by some thread.
  // The attached conditions are used to sync other threads (e.g., the one that is doing
  // memory eviction) that are interested in checkpointing the same file
  private final HashMap<Integer, Condition> mInProgressFiles = 
      new HashMap<Integer, Condition>(); 


  public CheckpointManager(BlockDataManager bdm, 
      BlockStore bs, 
      UnderFileSystem ufs, 
      //String chkpUserUfsDataFolder,
      int numCheckpointThreads,
      int checkpointCapacity) {

    mBlockDataManager = bdm;
    mBlockStore = bs;
    mUfs = ufs;
    mNumCheckpointThreads = numCheckpointThreads;
    mCheckpointCapacityPerSec = checkpointCapacity;
    mCheckpointExecutor = Executors.newScheduledThreadPool(
        mNumCheckpointThreads,
        ThreadFactoryUtils.build("checkpoint-%d", false));
  }

  /**
   * Schedule a new file for checkpointing. 
   *
   * @param fileInfo info about the fiel to be checkpointed (does it have any dependency?)
   * @param fileId the inode id of the file to be checkpointed
   */
  public void scheduleForCheckpoint(long userId, ClientFileInfo fileInfo) throws IOException {
    final int fileId = fileInfo.getId();
    if (fileInfo.getId() < 0) {
      throw new IllegalArgumentException("Invalid fileId reported from the master");
    }
    mLock.lock();
    try {
      if (mUncheckpointedFiles.containsKey(fileId)) {
        LOG.warn("File {} was already scheduled for checkpointing. Not scheduling again.", fileId);
        return;
      }

      mFifoUncheckpointedFiles.add(fileId);
      mUncheckpointedFiles.put(fileId, 
          new UncheckpointedFilesMapEntry(fileInfo));
      for (long blockId: fileInfo.getBlockIds()) {
        final long blockLock = mBlockStore.lockBlock(userId, blockId);
        try {
          final BlockMeta m = mBlockStore
              .getBlockMeta(userId, blockId, blockLock);
          LOG.debug("Marking block {} of file {} as non dirty. Before it was {}",
              blockId, blockId >> 30, m.isDirty());
          m.setDirty(false);
        } finally {
          mBlockStore.unlockBlock(blockLock);
        }
      }

    } finally {
      mLock.unlock();
    }

  }

  /**
   * Checks wether the file with inode fileId has been written back on UFS, and possibly
   * performs the actual writeback of the file.
   *
   * The calling thread will check wether the file with the given inodeid needs to be
   * written back or not. If it does need to be written on UFS, the callign thread will
   * perform the process. The method is thread safe.
   *
   * @param fileId a file InodedId
   * @return true either if the file has been successfully checkpointing (by the calling thread
   * or previously by some other thread), or if the file has never been scheduled for checkpointing.
   * False if the file has not been written back on
   * UFS and the current thread was unable to do so.
   */
  public boolean ensureCheckpointed(int fileId, long userId) {
    if (fileId < 0) {
      throw new IllegalArgumentException("Invalid negative fileId: " + fileId);
    }
    return checkpointFile(fileId, userId) >= 0;

  }
  /**
   * Start the asynchrnous checkpointing process.
   *
   * The CheckpointManager will use the executor service passed at construction time to
   * schedule periodic async checkpointing. Calling this method multiple times before calling
   * stop is idempotent.
   */
  public void start() {
    mLock.lock();
    try {
      if (!mStarted) {
        mStarted = true; 
        LOG.info("Starting checkpoint manager");
        for (int k = 0; k < mNumCheckpointThreads; k ++) {
          mCheckpointExecutor.submit(new CheckpointTask());
        }
      }
    } finally {
      mLock.unlock();
    }
  }

  /**
   * Stop the asynchronous checkpointing process.
   *
   * Note that tasks that were already scheduled will complete anyways.
   */
  public void stop() throws InterruptedException {
    mStarted = false;
    mCheckpointExecutor.shutdown();
    mCheckpointExecutor.awaitTermination(10, TimeUnit.SECONDS);
  }

  private long blocksToUfs(ClientFileInfo fileInfo, String ufsDst, 
      long userId) throws IOException {

    LOG.debug("Checkpointing: [fid: {}, uid: {}, blocks: {}]",
        fileInfo.getId(), userId, fileInfo.blockIds.toString());

    OutputStream os = null;
    long fileSizeByte = 0;
    final int numBlocks = fileInfo.blockIds.size();
    //final long[] blockLocks = new long[numBlocks];

    // for (int k = 0; k < numBlocks; k ++) {
    //   final long blockId = fileInfo.blockIds.get(k);
    //   blockLocks[k] = mBlockStore.lockBlock(userId, blockId);
    // }
    try {
      os = mUfs
        .create(ufsDst, (int) fileInfo.getBlockSizeByte());
      for (int k = 0; k < numBlocks; k ++) {
        final long blockId = fileInfo.blockIds.get(k);
        final long blockLock = mBlockStore.lockBlock(userId, blockId);
        final BlockReader rd = mBlockStore.getBlockReader(userId, 
            blockId, blockLock); 
        try {
          final ByteBuffer byteBuffer = rd.read(0, -1);
          final byte[] buf = new byte[16 * Constants.KB];
          int writeLen;
          while (byteBuffer.remaining() > 0) {
            if (byteBuffer.remaining() >= buf.length) {
              writeLen = buf.length;
            } else {
              writeLen = byteBuffer.remaining();
            }
            byteBuffer.get(buf, 0, writeLen);
            os.write(buf, 0, writeLen);
            fileSizeByte += writeLen;
          }
          CommonUtils.cleanDirectBuffer(byteBuffer);
        } finally {
          if ( rd != null ) {
            rd.close();
          }
          mBlockStore.unlockBlock(blockLock);
        }

      }
    } finally {
      // for (int k = 0; k < numBlocks; k ++) {
      //   mBlockStore.unlockBlock(blockLocks[k]);
      // }
      if (os != null) {
        os.close();
      }
    }

    LOG.debug("Successful checkpoint: [fid: {}, uid: {}, blocks: {}]",
        fileInfo.getId(), userId, fileInfo.blockIds.toString());

    return fileSizeByte;

  }

  // If fileId is not already written back, it performs its checkpoint.
  // Implements the thread-synchronization mechanisms so that multiple
  // threads may safely attempt to checkpoint the same file.
  // @return 0 if the file was already checkpointed by another thread
  // @return negative for error
  // @return positive if the checkpoint is successful. The return value
  // is the size of the checkpointed file.
  private long checkpointFile(int fileId, long userId) {
    UncheckpointedFilesMapEntry prevEntry;
    mLock.lock();
    try {
      // ensure there's no other thread working on the checkpoint of this
      // file
      if (mInProgressFiles.containsKey(fileId)) {
        final Condition fileCondition = mInProgressFiles.get(fileId);
        while (mInProgressFiles.containsKey(fileId)) {
          try {
            fileCondition.await();
          } catch (InterruptedException e) {
            LOG.warn("Thread {} was unexpectedly interrupted "
                + "while it was waiting for an in-progress checkpoint "
                + "(fileid={}).", Thread.currentThread().getName(), fileId);

          }
        }
      }

      // check wether the file is checkpointed
      if (!mUncheckpointedFiles.containsKey(fileId)) {
        return 0;
      }

      // if a thread arrives here, it must do the checkpointing
      // itself
      prevEntry = mUncheckpointedFiles.remove(fileId);
      // O(n) operation -- although this file is most of the times at the head of the lsit:(
      mFifoUncheckpointedFiles.removeFirstOccurrence(fileId);

      mInProgressFiles.put(fileId, mLock.newCondition());
    } finally {
      mLock.unlock();
    }

    // do the checkpointing. Note that due to the locking scheme
    // (see mInProgressFiles) no two threads can do the checkpointing
    // for the same file at the same time
    long successful = doCheckpointFile(prevEntry.mFileInfo, userId);
    

    // reacquire the global lock
    mLock.lock();
    try {
      if (successful < 0) {
        prevEntry.mRetries += 1;
        if ( prevEntry.mRetries < CheckpointManager.MAX_CHECKPOINT_RETRIES) {
          // checkpointing unsuccessfull, next guy will retry
          mUncheckpointedFiles.put(fileId, prevEntry); 
          mFifoUncheckpointedFiles.addFirst(fileId);

        } else {
          LOG.error("Giving up checkpointing file {}: " 
              + "maximum number of retries exceeded.", fileId);
        }
      } 
      // declare that the thread has finished to work with the file
      final Condition fileCondition = mInProgressFiles.remove(fileId);
      // possibly awake other threads 
      fileCondition.signalAll();

      return successful; 
    } finally {
      mLock.unlock();
    }
  }

  
  private long doCheckpointFile(ClientFileInfo fileInfo, long userId) {
    final int fileId = fileInfo.getId();
    // TODO checkpoint process. In future, move from midPath to dstPath should be done by
    // master
    try {
      final String midPath = mBlockDataManager.getUserUfsTmpFolder(userId);
      final String ufsPath = CommonUtils.concatPath(midPath, fileId);

      // TODO: 2015-02-23 Andrea: I think that nobody is checking wether the ids generated
      // by the master do not wrap araound (they are generated through an AtomicInteger).
      // However the code extensively used id == -1 as an error code.
      if (fileInfo.getId() < 0 ) {
        LOG.error("File {} is unrecognized by the master.", fileId);
        return -1L;
      }

      long fileSizeByte = blocksToUfs(fileInfo, ufsPath, userId);

      try {
        mBlockDataManager.addCheckpoint(userId, fileId);
      } catch (FileDoesNotExistException e) {
        // file must have been deleted (this happens, for examples when a job
        // abborts while the file is still being checkpoitned)
        LOG.warn("File {} has been deleted on the master. "
            + "Deleting the checkpoint from {}", fileId, ufsPath);
        mUfs.delete(ufsPath, false);
      }


      return fileSizeByte;

    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      return -1;
    } catch (TException e ) {
      LOG.error(e.getMessage(), e);
      return -1;
    } 
  }


  private int getFifoUncheckpointedFile() {
    if (mUncheckpointedFiles.isEmpty()) {
      return -1;
    } else {
      int fileId = mFifoUncheckpointedFiles.peekFirst();
      return fileId;
    }
  }

  private int selectNextCheckpoint() {
    int fileId = -1;
    mLock.lock();
    try {
      fileId = getFifoUncheckpointedFile();
    } finally {
      mLock.unlock();
    }

    return fileId;
  }

  private final class CheckpointTask implements Runnable {
    public void run() {
      if (!mStarted) {
        return;
      }
      final int fileId = selectNextCheckpoint();
      // TODO: very bad. Create an empty list condition where to stop
      if (fileId == -1) {
        // LOG.debug("{} has nothing to checkpoint. Sleep for 1 sec.", 
        //     Thread.currentThread().getName());
        mCheckpointExecutor.schedule(this, 
            Constants.SECOND_MS, TimeUnit.MILLISECONDS);
        return;
      }

      final long startCopyTimeMs = System.currentTimeMillis();
      final long fileSizeByte = checkpointFile(fileId, Users.CHECKPOINT_USER_ID);
      final long currentTimeMs = System.currentTimeMillis();
      final long timeTaken = currentTimeMs - startCopyTimeMs;

      long shouldTakeMs = (long) (1000.0 * fileSizeByte / Constants.MB
          / mCheckpointCapacityPerSec);

      if (fileSizeByte < 0 || timeTaken > shouldTakeMs) {
        mCheckpointExecutor.submit(this);
      } else {
        long shouldSleepMs = shouldTakeMs - timeTaken;
        LOG.info("Checkpointing last file {} took {} ms. Need to sleep {} ms.", 
            fileId,(timeTaken), shouldSleepMs);
        mCheckpointExecutor.schedule(this, shouldSleepMs, 
            TimeUnit.MILLISECONDS);
      } 
    }
  }

}
