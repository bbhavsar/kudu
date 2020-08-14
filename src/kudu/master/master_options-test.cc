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

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/client/client.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/master/catalog_manager.h"
#include "kudu/master/master.h"
#include "kudu/master/master.pb.h"
#include "kudu/master/master.proxy.h"
#include "kudu/master/mini_master.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/mini-cluster/internal_mini_cluster.h"
#include "kudu/mini-cluster/mini_cluster.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"

using kudu::client::KuduClient;
using kudu::client::sp::shared_ptr;
using kudu::cluster::ExternalMiniCluster;
using kudu::cluster::ExternalMiniClusterOptions;
using kudu::cluster::InternalMiniCluster;
using kudu::cluster::InternalMiniClusterOptions;
using kudu::cluster::MiniCluster;
using std::string;
using std::unique_ptr;

namespace kudu {
namespace master {
class MasterOptionsTest : public KuduTest {
};

// Test bringing up a cluster with a single master config by populating --master_addresses flag
// with a single address.
TEST_F(MasterOptionsTest, TestSingleMasterWithMasterAddresses) {
  // Reserving a port upfront for the master so that its address can be specified in
  // --master_addresses.
  unique_ptr<Socket> reserved_socket;
  ASSERT_OK(MiniCluster::ReserveDaemonSocket(MiniCluster::MASTER, 1 /* index */,
                                             kDefaultBindMode, &reserved_socket));
  Sockaddr reserved_addr;
  ASSERT_OK(reserved_socket->GetSocketAddress(&reserved_addr));
  string reserved_hp_str = HostPort(reserved_addr).ToString();

  // ExternalMiniCluster closely simulates a real cluster where MasterOptions
  // is constructed from the supplied flags.
  ExternalMiniClusterOptions opts;
  opts.num_masters = 1;
  opts.extra_master_flags = { "--master_addresses=" + reserved_hp_str };

  ExternalMiniCluster cluster(opts);
  ASSERT_OK(cluster.Start());
  ASSERT_OK(cluster.WaitForTabletServerCount(cluster.num_tablet_servers(),
                                             MonoDelta::FromSeconds(5)));
  shared_ptr<KuduClient> client;
  ASSERT_OK(cluster.CreateClient(nullptr, &client));

  auto verification_steps = [&] {
    ASSERT_EQ(reserved_hp_str, client->GetMasterAddresses());
    ASSERT_FALSE(client->IsMultiMaster());

    ListMastersRequestPB req;
    ListMastersResponsePB resp;
    rpc::RpcController rpc;
    ASSERT_OK(cluster.master_proxy()->ListMasters(req, &resp, &rpc));
    ASSERT_EQ(1, resp.masters_size());
    ASSERT_EQ(consensus::RaftPeerPB::LEADER, resp.masters(0).role());
  };
  verification_steps();

  // Restarting the cluster exercises loading the existing system catalog code-path.
  ASSERT_OK(cluster.Restart());
  ASSERT_OK(cluster.WaitForTabletServerCount(cluster.num_tablet_servers(),
                                             MonoDelta::FromSeconds(5)));
  verification_steps();
}

// Test that verifies the 'last_known_addr' field in Raft config is set with a single master
// configuration when '--master_addresses' field is supplied.
TEST_F(MasterOptionsTest, TestSingleMasterForRaftConfig) {
  InternalMiniClusterOptions opts;
  opts.num_masters = 1;
  InternalMiniCluster cluster(env_, opts);
  // '--master_addresses' field is implicitly specified by InternalMiniCluster on starting masters
  // even for single master configuration.
  ASSERT_OK(cluster.Start());
  auto consensus = cluster.mini_master()->master()->catalog_manager()->master_consensus();
  ASSERT_NE(nullptr, consensus);
  auto config = consensus->CommittedConfig();
  ASSERT_EQ(1, config.peers_size());
  ASSERT_TRUE(config.peers(0).has_last_known_addr());
}

} // namespace master
} // namespace kudu
