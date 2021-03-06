/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker;

import alluxio.exception.ConnectionFailedException;
import alluxio.master.block.BlockMaster;
import alluxio.thrift.Command;
import alluxio.thrift.CommandType;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterSync;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The single place to get, set, and update worker id.
 *
 * When worker process starts in {@link AlluxioWorker}, worker is temporarily lost from master,
 * or when leader master is down in fault tolerant mode, this class will try to get a new worker id
 * from {@link BlockMaster}.
 *
 * Worker id will only be regained in {@link BlockMasterSync} when it receives {@link Command} with
 * type {@link CommandType#Register} from {@link BlockMaster}.
 *
 * This class should be the only source of current worker id within the same worker process.
 */
@NotThreadSafe
public final class WorkerIdRegistry {
  /**
   * The default value to initialize worker id, the worker id generated by master will never be the
   * same as this value.
   */
  public static final long INVALID_WORKER_ID = 0;
  private static AtomicLong sWorkerId = new AtomicLong(INVALID_WORKER_ID);

  private WorkerIdRegistry() {}

  /**
   * Registers with {@link BlockMaster} to get a new worker id.
   *
   * @param masterClient the master client to be used for RPC
   * @param workerAddress current worker address
   * @throws IOException when fails to get a new worker id
   * @throws ConnectionFailedException if network connection failed
   */
  public static void registerWithBlockMaster(BlockMasterClient masterClient,
      WorkerNetAddress workerAddress) throws IOException, ConnectionFailedException {
    sWorkerId.set(masterClient.getId(workerAddress));
  }

  /**
   * @return worker id, 0 is invalid worker id, representing that the worker hasn't been registered
   *         with master
   */
  public static Long getWorkerId() {
    return sWorkerId.get();
  }
}
