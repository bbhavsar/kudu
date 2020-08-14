// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "kudu/client/client-test-util.h"
#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalDaemonOptions;
using kudu::cluster::ExternalMaster;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::MiniCluster;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace master {

class DynamicMultiMasterTest : public KuduTest {
};

static Status CreateTable(ExternalMiniCluster* cluster,
                          const std::string& table_name) {
  shared_ptr<KuduClient> client;
      RETURN_NOT_OK(cluster->CreateClient(nullptr, &client));
  KuduSchema schema;
  KuduSchemaBuilder b;
  b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
      RETURN_NOT_OK(b.Build(&schema));
  unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  return table_creator->table_name(table_name)
      .schema(&schema)
      .set_range_partition_columns({ "key" })
      .num_replicas(1)
      .Create();
}

// This test starts a cluster with 2 masters, creates a table and then adds 3rd master.
// For a system catalog with little data, the new master can be caught up from
// WAL not requiring a tablet copy.
TEST_F(DynamicMultiMasterTest, TestAddMasterCatchupFromWAL) {
  int num_masters = 2;
  // Reserving a port upfront for 3rd master that'll be added to the cluster.
  unique_ptr<Socket> reserved_socket;
  ASSERT_OK(MiniCluster::ReserveDaemonSocket(MiniCluster::MASTER, num_masters /* index */,
                                             kDefaultBindMode, &reserved_socket));
  Sockaddr reserved_addr;
  ASSERT_OK(reserved_socket->GetSocketAddress(&reserved_addr));
  HostPort reserved_hp = HostPort(reserved_addr);
  const string reserved_hp_str = reserved_hp.ToString();

  ExternalMiniClusterOptions opts;
  opts.num_masters = num_masters;
  opts.extra_master_flags = {"--master_support_change_config"};

  unique_ptr<ExternalMiniCluster> cluster(new ExternalMiniCluster(opts));
  ASSERT_OK(cluster->Start());
  ASSERT_OK(cluster->WaitForTabletServerCount(cluster->num_tablet_servers(),
                                              MonoDelta::FromSeconds(5)));
  int leader_master;

  // Verify that masters are running as VOTERs and collect their addresses to be used
  // on starting the 3rd master.
  vector<HostPort> master_hps;
  vector<string> master_addresses;
  {
    ListMastersRequestPB req;
    ListMastersResponsePB resp;
    rpc::RpcController rpc;

    ASSERT_OK(cluster->GetLeaderMasterIndex(&leader_master));
    ASSERT_LT(leader_master, num_masters);
    ASSERT_OK(cluster->master_proxy(leader_master)->ListMasters(req, &resp, &rpc));
    ASSERT_EQ(num_masters, resp.masters_size());
    for (const auto& master : resp.masters()) {
      ASSERT_TRUE(master.role() == consensus::RaftPeerPB::LEADER ||
                  master.role() == consensus::RaftPeerPB::FOLLOWER);
      ASSERT_EQ(consensus::RaftPeerPB::VOTER, master.member_type());
      ASSERT_EQ(1, master.registration().rpc_addresses_size());
      master_hps.emplace_back(HostPortFromPB(master.registration().rpc_addresses(0)));
    }
    master_hps.emplace_back(reserved_hp);
  }
  std::transform(master_hps.begin(), master_hps.end(), std::back_inserter(master_addresses),
                 [](const HostPort& hp) {
                    return hp.ToString();
                 });

  const string kTableName = "first_table";
  ASSERT_OK(CreateTable(cluster.get(), kTableName));

  // Bring up the 3rd master to be added.
  ExternalDaemonOptions new_master_opts;
  const string new_master_id = Substitute("master-$0", num_masters);
  new_master_opts.exe = cluster->GetBinaryPath("kudu-master");
  new_master_opts.messenger = cluster->messenger();
  new_master_opts.block_manager_type = cluster->block_manager_type();
  new_master_opts.wal_dir = cluster->GetWalPath(new_master_id);
  new_master_opts.data_dirs = cluster->GetDataPaths(new_master_id);
  new_master_opts.log_dir = cluster->GetLogPath(new_master_id);
  new_master_opts.rpc_bind_address = reserved_hp;
  new_master_opts.start_process_timeout = cluster->start_process_timeout();
  new_master_opts.extra_flags = {
      "--master_addresses=" + JoinStrings(master_addresses, ","),
      "--rpc_reuseport=true",
      "--master_support_change_config",
      "--master_address_add_new_master=" + reserved_hp_str,
      "--logtostderr",
      "--logbuflevel=-1"
  };

  LOG(INFO) << "Bringing up the new master at: " << reserved_hp_str;
  scoped_refptr<ExternalMaster> new_master(new ExternalMaster(new_master_opts));
  ASSERT_OK(new_master->Start());
  ASSERT_OK(new_master->WaitForCatalogManager());

  // Verify the new master is a LEARNER and NON_VOTER before adding it to the cluster.
  unique_ptr<MasterServiceProxy> new_master_proxy(
      new MasterServiceProxy(new_master_opts.messenger, reserved_addr, reserved_addr.host()));
  {
    GetMasterRegistrationRequestPB req;
    GetMasterRegistrationResponsePB resp;
    rpc::RpcController rpc;

    ASSERT_OK(new_master_proxy->GetMasterRegistration(req, &resp, &rpc));
    ASSERT_EQ(consensus::RaftPeerPB::LEARNER, resp.role());
    ASSERT_EQ(consensus::RaftPeerPB::NON_VOTER, resp.member_type());
  }

  // Verify the cluster still has 2 masters.
  {
    ListMastersRequestPB req;
    ListMastersResponsePB resp;
    rpc::RpcController rpc;

    ASSERT_OK(cluster->GetLeaderMasterIndex(&leader_master));
    ASSERT_OK(cluster->master_proxy(leader_master)->ListMasters(req, &resp, &rpc));
    ASSERT_EQ(num_masters, resp.masters_size());
  }

  // Add the new master.
  {
    AddMasterRequestPB req;
    AddMasterResponsePB resp;
    rpc::RpcController rpc;
    *req.mutable_rpc_addr() = HostPortToPB(reserved_hp);

    ASSERT_OK(cluster->GetLeaderMasterIndex(&leader_master));
    ASSERT_OK(cluster->master_proxy(leader_master)->AddMaster(req, &resp, &rpc));
    num_masters++;
  }

  // Newly added master will be caught up from WAL itself without requiring tablet copy
  // since the system catalog is fresh with a single table.
  ASSERT_EVENTUALLY([&] {
    ListMastersRequestPB req;
    ListMastersResponsePB resp;
    rpc::RpcController rpc;
    ASSERT_OK(cluster->GetLeaderMasterIndex(&leader_master));
    ASSERT_OK(cluster->master_proxy(leader_master)->ListMasters(req, &resp, &rpc));
    ASSERT_EQ(num_masters, resp.masters_size());

    for (const auto& master : resp.masters()) {
      ASSERT_EQ(consensus::RaftPeerPB::VOTER, master.member_type());
      ASSERT_TRUE(master.role() == consensus::RaftPeerPB::LEADER ||
                  master.role() == consensus::RaftPeerPB::FOLLOWER);
    }
  });

  // Double check by directly contacting the new master.
  {
    GetMasterRegistrationRequestPB req;
    GetMasterRegistrationResponsePB resp;
    rpc::RpcController rpc;

    ASSERT_OK(new_master_proxy->GetMasterRegistration(req, &resp, &rpc));
    ASSERT_TRUE(resp.role() == consensus::RaftPeerPB::FOLLOWER ||
                resp.role() == consensus::RaftPeerPB::LEADER);
    ASSERT_EQ(consensus::RaftPeerPB::VOTER, resp.member_type());
  }

  // Adding the same master again should return an error.
  {
    AddMasterRequestPB req;
    AddMasterResponsePB resp;
    rpc::RpcController rpc;
    *req.mutable_rpc_addr() = HostPortToPB(reserved_hp);

    ASSERT_OK(cluster->GetLeaderMasterIndex(&leader_master));
    ASSERT_TRUE(
        cluster->master_proxy(leader_master)->AddMaster(req, &resp, &rpc).IsRemoteError());
    ASSERT_STR_CONTAINS(rpc.status().message().ToString(), "Master already present");
  }

  // Shutdown the cluster and the new master daemon process.
  // This allows ExternalMiniCluster to manage the newly added master and allows
  // client to connect to the new master if it's elected the leader.
  new_master->Shutdown();
  cluster.reset();

  opts.num_masters = num_masters;
  opts.master_rpc_addresses = master_hps;
  ExternalMiniCluster migrated_cluster(std::move(opts));
  ASSERT_OK(migrated_cluster.Start());
  ASSERT_OK(migrated_cluster.WaitForTabletServerCount(migrated_cluster.num_tablet_servers(),
                                                      MonoDelta::FromSeconds(5)));

  // Verify the cluster still has the same 3 masters.
  {
    ListMastersRequestPB req;
    ListMastersResponsePB resp;
    rpc::RpcController rpc;

    ASSERT_OK(migrated_cluster.GetLeaderMasterIndex(&leader_master));
    ASSERT_OK(migrated_cluster.master_proxy(leader_master)->ListMasters(req, &resp, &rpc));
    ASSERT_EQ(num_masters, resp.masters_size());

    for (const auto& master : resp.masters()) {
      ASSERT_EQ(consensus::RaftPeerPB::VOTER, master.member_type());
      ASSERT_TRUE(master.role() == consensus::RaftPeerPB::LEADER ||
                  master.role() == consensus::RaftPeerPB::FOLLOWER);
      ASSERT_EQ(1, master.registration().rpc_addresses_size());
      HostPort actual_hp = HostPortFromPB(master.registration().rpc_addresses(0));
      ASSERT_TRUE(std::find(master_hps.begin(), master_hps.end(), actual_hp) != master_hps.end());
    }
  }

  shared_ptr<KuduClient> client;
  ASSERT_OK(migrated_cluster.CreateClient(nullptr, &client));

  shared_ptr<KuduTable> table;
  ASSERT_OK(client->OpenTable(kTableName, &table));
  ASSERT_EQ(0, CountTableRows(table.get()));

  // Perform an operation that requires replication to masters.
  ASSERT_OK(CreateTable(&migrated_cluster, "second_table"));
  ASSERT_OK(client->OpenTable("second_table", &table));
  ASSERT_EQ(0, CountTableRows(table.get()));

  // Pause master one at a time and create table at the same time which will allow
  // new leader to be elected if the paused master is a leader.
  for (int i = 0; i < num_masters; i++) {
    ASSERT_OK(migrated_cluster.master(i)->Pause());
    cluster::ScopedResumeExternalDaemon resume_daemon(migrated_cluster.master(i));
    ASSERT_OK(client->OpenTable(kTableName, &table));
    ASSERT_EQ(0, CountTableRows(table.get()));

    // See MasterFailoverTest.TestCreateTableSync to understand why we must
    // check for IsAlreadyPresent as well.
    Status s = CreateTable(&migrated_cluster, Substitute("table-$0", i));
    ASSERT_TRUE(s.ok() || s.IsAlreadyPresent());
  }
}

} // namespace master
} // namespace kudu
