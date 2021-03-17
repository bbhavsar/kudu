#!/bin/bash -e
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Script to automate the workflow of adding a master to an existing Kudu cluster.
# See the usage description below for prerequisites for running this script.
usage() {
  cat <<EOF
Script to add a master to an existing Kudu cluster.

This script must be run on the new master being added and NOT on any of the
existing masters of the cluster.

This script requires following preconditions:
- The new master has been brought up with --master_addresses that includes
  existing masters in the cluster along with itself.
- Flag --master_address_add_new_master is set to 'hostname:port' of the new
  master address
- The masters in existing cluster and the new master have the flag
  --master_support_change_config set to true

Usage: add_master.sh <args>

Positional arguments:
kudu-executable     Path to the Kudu executable.
master-addresses    Comma-separated list of Kudu master addresses where each
                    address is of form 'hostname:port'
new-master-address  Address of a Kudu Master of form 'hostname:port'
fs-wal-dir          WAL directory of the new master
fs-data-dirs        Comma-separated list of data directories of the new master
stop-master         Optional boolean('true'/'false') parameter whether the new
                    master should be stopped after completion of the procedure.
                    Default: 'false'.

Example: add_master.sh \
         /path/to/kudu \
         master-1:1234,master-2:1234 \
         new_master_host:1234 \
         /new-master/path/to/fs-wal-dir \
         /new-master/path/to/data-dirs

EOF
}

# Constants
SYS_CATALOG_UUID="00000000000000000000000000000000"
MAX_KSCK_ATTEMPTS=10

log() {
  timestamp=$(date)
  echo "$timestamp: $1"
}

# Check the supplied variable is set.
check_var_set() {
  if [ -z "$2" ]; then
    log "Variable $1 not set"
    exit 1
  fi
}

# Check the supplied variable is set with a single line.
check_var_set_and_single_line() {
  check_var_set "$1" "$2"
  local NUM_LINES
  NUM_LINES=$(wc -l <<<"$2")
  if [ "$NUM_LINES" -ne 1 ]; then
    log "Variable $1 has $NUM_LINES lines when expecting a single line"
    exit 1
  fi
}

# Validate and set the input parameters
validate_and_set_global_params() {
  NUM_REQUIRED_PARAMS=5
  NUM_OPTIONAL_PARAMS=1
  NUM_TOTAL_PARAMS=$(($NUM_REQUIRED_PARAMS + $NUM_OPTIONAL_PARAMS))
  if [ "$#" -ne "$NUM_REQUIRED_PARAMS" ] && [ "$#" -ne "$NUM_TOTAL_PARAMS" ]; then
    log "Incorrect number of parameters: $#, expected at least : $NUM_REQUIRED_PARAMS"
    usage
    exit 1
  fi

  KUDU_BINARY="$1"
  MASTER_ADDRESSES="$2"
  NEW_MASTER_HP="$3"
  FS_WAL_DIR="$4"
  FS_DATA_DIRS="$5"
  STOP_MASTER=$([ "$#" -ge 6 ] && echo "$6" || echo "false")

  if [ ! -f "$KUDU_BINARY" ]; then
    log "Cannot find $KUDU_BINARY executable"
    exit 1
  fi

  if [ ! -d "$FS_WAL_DIR" ]; then
    log "$FS_WAL_DIR is not a directory"
    exit 1
  fi

  for DIR in $(echo "$FS_DATA_DIRS" | sed "s/,/\n/g"); do
    if [ ! -d "$DIR" ]; then
      log "$DIR is not a directory"
      exit 1
    fi
  done

  if [ "$STOP_MASTER" != "true" ] && [ "$STOP_MASTER" != "false" ]; then
    log "Unexpected value of optional 'stop_master' boolean parameter. Expected 'true' or 'false'."
    exit 1
  fi
}

# Fetch the local uuid of the new master
fetch_local_master_uuid() {
  NEW_MASTER_UUID=$("$KUDU_BINARY" local_replica cmeta print_replica_uuids "$SYS_CATALOG_UUID" \
                    --fs_wal_dir="$FS_WAL_DIR" --fs_data_dirs="$FS_DATA_DIRS" |& grep uuid | \
                    cut -d':' -f2 | tr -d '[:blank:][:punct:]')
  log "New master UUID: $NEW_MASTER_UUID"
  check_var_set_and_single_line "NEW_MASTER_UUID" "$NEW_MASTER_UUID"
}

# Fetch the PID of the the new master. This helps in case we need to restart the master.
fetch_local_master_pid() {
  # Using port helps uniquely determine kudu master on a test environment that has
  # multiple kudu masters running locally.
  local NEW_MASTER_PORT
  NEW_MASTER_PORT=$(cut -d':' -f2 <<<"$NEW_MASTER_HP")
  check_var_set_and_single_line "NEW_MASTER_PORT" "$NEW_MASTER_PORT"
  log "New master port: $NEW_MASTER_PORT"

  NEW_MASTER_PID=$(lsof -t -i :"$NEW_MASTER_PORT" -s TCP:LISTEN)
  check_var_set_and_single_line "NEW_MASTER_PID" "$NEW_MASTER_PID"
  log "New master PID: $NEW_MASTER_PID"
}

# Fetch the flags of the new master. This helps in case we need to restart the master.
fetch_new_master_flags() {
  NEW_MASTER_FLAGS=$("$KUDU_BINARY" master get_flags "$NEW_MASTER_HP" | tail -n+3 | \
                     cut -d'|' -f1,2 | tr -d "[:blank:]" | sed "s/^/--/g" | sed "s/|/=/g")
  check_var_set "NEW_MASTER_FLAGS" "$NEW_MASTER_FLAGS"
}

# Verify new master has been started with master addresses of existing cluster including itself
verify_new_master_with_master_addresses() {
  local EXPECTED_MASTER_ADDRESSES
  if [[ "$MASTER_ADDRESSES" != *"$NEW_MASTER_HP"* ]]; then
    EXPECTED_MASTER_ADDRESSES="$MASTER_ADDRESSES,$NEW_MASTER_HP"
  else
    EXPECTED_MASTER_ADDRESSES="$MASTER_ADDRESSES"
  fi
  local LHS
  LHS=$(echo "$EXPECTED_MASTER_ADDRESSES" | sed "s/,/\n/g" | sort | uniq)
  local RHS
  RHS=$(echo "$NEW_MASTER_FLAGS" | grep "master_addresses" | cut -d"=" -f2 | sed "s/,/\n/g" | \
        sort | uniq)
  if [ "$LHS" != "$RHS" ]; then
    log "New master not started with correct list of master addresses: $RHS"
    log "New master must be started with fresh filesystem directories with master addresses of "
        "cluster including itself."
    exit 1
  fi
}

# Check that the system catalog on the new master is empty before proceeding.
verify_sys_catalog_empty() {
  local SYS_CAT_SIZE
  SYS_CAT_SIZE=$("$KUDU_BINARY" local_replica data_size "$SYS_CATALOG_UUID" \
                 --fs_wal_dir="$FS_WAL_DIR" --fs_data_dirs="$FS_DATA_DIRS" | tail -n1 | \
                 cut -d'|' -f5 | tr -d '[:blank:] [:alpha:]')
  log "System catalog size on new master: $SYS_CAT_SIZE"
  check_var_set_and_single_line "SYS_CATALOG_SIZE" "$SYS_CAT_SIZE"

  # Using python to compare floating point numbers.
  local IS_SYS_CAT_EMPTY
  IS_SYS_CAT_EMPTY=$(python -c "out = 1 if float($SYS_CAT_SIZE) <= 0.001 else 0; print(out)")
  if [ "$IS_SYS_CAT_EMPTY" -eq "0" ]; then
    log "System catalog on the new master $NEW_MASTER_HP not empty."
    log "This script must be run from the new master being added."
    exit 1
  fi
}

# Add new master to Raft configuration using the 'kudu master add' CLI.
do_add_master() {
  # kudu master add CLI prints useful error messages which we want to relay back to the user.
  # Hence toggling the exit on error bash directive.
  set +e
  log "Adding master $NEW_MASTER_HP to the cluster"
  ADD_MASTER_OUTPUT=$( ("$KUDU_BINARY" master add "$MASTER_ADDRESSES" "$NEW_MASTER_HP") 2>&1 )
  local ADD_MASTER_EXIT_STATUS=$?
  if [ "$ADD_MASTER_EXIT_STATUS" -ne 0 ]; then
    log "$ADD_MASTER_OUTPUT"
    exit $ADD_MASTER_EXIT_STATUS
  fi
  check_var_set "ADD_MASTER_OUTPUT" "$ADD_MASTER_OUTPUT"
  log "$ADD_MASTER_OUTPUT"

  if [[ "$ADD_MASTER_OUTPUT" != *"Successfully added master"* ]]; then
    log "Master successful addition message not found!"
    exit 1
  fi
  set -e
}

# Update cluster master addresses after successful addition of the master to the Raft configuration.
update_master_addresses() {
  # This could be a retry and ksck crashes if duplicate master addresses are found.
  if [[ "$MASTER_ADDRESSES" != *"$NEW_MASTER_HP"* ]]; then
    UPDATED_MASTER_ADDRESSES="$MASTER_ADDRESSES,$NEW_MASTER_HP"
  else
    UPDATED_MASTER_ADDRESSES="$MASTER_ADDRESSES"
  fi

  log "UPDATED_MASTER_ADDRESSES: $UPDATED_MASTER_ADDRESSES"
}

# Run a master specific ksck to verify state of masters.
run_ksck_master() {
  local KSCK_JSON_STR
  KSCK_JSON_STR=$("$KUDU_BINARY" cluster ksck "$UPDATED_MASTER_ADDRESSES" \
                  -sections MASTER_SUMMARIES -ksck_format=json_pretty)
  check_var_set "KSCK_JSON_STR" "$KSCK_JSON_STR"

  # json output format is easier to parse and extract information and hence using python.
  local PYTHON_CODE
  PYTHON_CODE=$(
    cat <<END
import json
import sys

uuid = "$NEW_MASTER_UUID"
ksck_str = """
$KSCK_JSON_STR
"""

data = json.loads(ksck_str)

for summary in data["master_summaries"]:
  if summary["uuid"] == uuid:
    if summary["health"] != "HEALTHY":
      sys.exit("New master is not healthy!")

if data["master_consensus_conflict"]:
  sys.exit("Master consensus has conflicts")

for state in data["master_consensus_states"]:
  if state["type"] != "COMMITTED":
    sys.exit("One of the masters doesn't have committed Raft consensus state")
  if uuid not in state["voter_uuids"]:
    sys.exit("New master not a VOTER yet in at least one of the masters")

print("New master is HEALTHY and part of the cluster as VOTER.")
END
  )
  python -c "$PYTHON_CODE"
  return $?
}

# Wrapper function to retry running ksck on masters.
retry_ksck_master() {
  # Turning off exit on failure of subshell command as we retry ksck.
  set +e
  local ATTEMPT=1
  while [[ $ATTEMPT -le $MAX_KSCK_ATTEMPTS ]]; do
    log "Running ksck for master summaries. Attempt: $ATTEMPT"
    if run_ksck_master; then
      break
    fi
    sleep 1
    ((ATTEMPT++))
  done
  set -e

  if [[ $ATTEMPT -gt $MAX_KSCK_ATTEMPTS ]]; then
    log "ksck master failed after multiple attempts"
    exit 1
  fi

  log "Successfully completed ksck for masters"
}

# Check whether the new master has caught up from WAL. If so, we are done and the script exits
# successfully.
exit_if_new_master_caught_up_from_wal() {
  if [[ "$ADD_MASTER_OUTPUT" == *"Master $NEW_MASTER_HP successfully caught up from WAL"* ]]
  then
    log "System catalog copy not needed as master successfully caught up from WAL"
    retry_ksck_master
    log "Master $NEW_MASTER_HP successfully added to the cluster"
    exit 0
  elif [[ "$ADD_MASTER_OUTPUT" == *"Master $NEW_MASTER_HP could not be caught up from WAL"* ]]
  then
    log "System catalog copy needed"
  else
    log "Unexpected message from kudu master add CLI!"
    exit 1
  fi
}

# Shutdown the master and delete system catalog.
shutdown_delete_local_empty_sys_catalog() {
  log "Shutting down the new master"
  kill "$NEW_MASTER_PID"

  log "Deleting the system catalog"
  "$KUDU_BINARY" local_replica delete "$SYS_CATALOG_UUID" --fs_wal_dir="$FS_WAL_DIR" \
                 --fs_data_dirs="$FS_DATA_DIRS" -clean_unsafe
}

copy_sys_catalog_from_remote() {
  # Select any existing master from the cluster ensuring it's not the same as the new master
  # in case of a retry.
  local SRC_MASTER_ADDRESS=
  for ADDR in $(echo "$MASTER_ADDRESSES" | sed "s/,/\n/g"); do
    if [ "$ADDR" != "$NEW_MASTER_HP" ]; then
      SRC_MASTER_ADDRESS=$ADDR
      break
    fi
  done
  if [ -z "$SRC_MASTER_ADDRESS" ]; then
    log "No suitable source master found to copy system catalog"
    exit 1
  fi
  log "Copying from remote master: $SRC_MASTER_ADDRESS"
  "$KUDU_BINARY" local_replica copy_from_remote "$SYS_CATALOG_UUID" "$SRC_MASTER_ADDRESS" \
                 --fs_wal_dir="$FS_WAL_DIR" --fs_data_dirs="$FS_DATA_DIRS"
}

restart_and_verify_new_master() {
  log "Restarting the new master"
  # NEW_MASTER_FLAGS is a multi-line string variable and we want it to be split instead of
  # being considered as a single string, hence no double quoting.
  "$KUDU_BINARY" $NEW_MASTER_FLAGS master run &
  NEW_MASTER_RESTARTED_PID=$!
  log "Master running as pid: $NEW_MASTER_RESTARTED_PID"
  retry_ksck_master
}

main() {
  # Validation of input parameters and setting variables that'll be used later in the procedure.
  validate_and_set_global_params "$@"
  fetch_local_master_uuid
  fetch_local_master_pid
  fetch_new_master_flags
  verify_new_master_with_master_addresses
  verify_sys_catalog_empty

  do_add_master
  update_master_addresses

  # Script will exit in the function below if system catalog copy is not needed.
  exit_if_new_master_caught_up_from_wal

  shutdown_delete_local_empty_sys_catalog
  copy_sys_catalog_from_remote
  restart_and_verify_new_master

  # This is a workaround for test purposes so that Subprocess:Call() can return.
  # This is not expected to be used by customers.
  if [ "$STOP_MASTER" == "true" ]; then
    log "Stopping the new master"
    kill $NEW_MASTER_RESTARTED_PID
  fi

  log "Master $NEW_MASTER_HP successfully added to the cluster"
}

main "$@"
