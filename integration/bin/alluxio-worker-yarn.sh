#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
source "${SCRIPT_DIR}/common.sh"
ALLUXIO_WORKER_JAVA_OPTS="${ALLUXIO_WORKER_JAVA_OPTS:-${ALLUXIO_JAVA_OPTS}}"

# ${ALLUXIO_WORKER_MEMORY_SIZE} needs to be set to mount ramdisk
echo "Mounting ramdisk of ${ALLUXIO_WORKER_MEMORY_SIZE} MB on Worker"
${ALLUXIO_HOME}/bin/alluxio-mount.sh SudoMount

# Yarn will set LOG_DIRS to point to the Yarn application log directory
YARN_LOG_DIR="$LOG_DIRS"

echo "Formatting Alluxio Worker"

"${JAVA}" -cp "${CLASSPATH}" \
  ${ALLUXIO_WORKER_JAVA_OPTS} \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logger.type="WORKER_LOGGER" \
  -Dalluxio.logs.dir="${YARN_LOG_DIR}" \
  alluxio.Format WORKER > "${YARN_LOG_DIR}"/worker.out 2>&1

echo "Starting Alluxio Worker"

"${JAVA}" -cp "${CLASSPATH}" \
  ${ALLUXIO_WORKER_JAVA_OPTS} \
  -Dalluxio.home="${ALLUXIO_HOME}" \
  -Dalluxio.logger.type="WORKER_LOGGER" \
  -Dalluxio.logs.dir="${YARN_LOG_DIR}" \
  -Dalluxio.master.hostname="${ALLUXIO_MASTER_ADDRESS}" \
  alluxio.worker.AlluxioWorker >> "${YARN_LOG_DIR}"/worker.out 2>&1
