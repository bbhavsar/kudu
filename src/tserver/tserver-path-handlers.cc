// Copyright (c) 2013, Cloudera, inc.

#include "tserver/tserver-path-handlers.h"

#include <string>
#include <tr1/memory>
#include <vector>

#include "gutil/map-util.h"
#include "gutil/strings/human_readable.h"
#include "gutil/strings/substitute.h"
#include "server/webui_util.h"
#include "tablet/tablet.pb.h"
#include "tablet/tablet_bootstrap.h"
#include "tablet/tablet_peer.h"
#include "tserver/tablet_server.h"
#include "tserver/ts_tablet_manager.h"
#include "util/url-coding.h"

using kudu::consensus::TransactionStatusPB;
using kudu::tablet::TabletPeer;
using kudu::tablet::TabletStatusPB;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tserver {

TabletServerPathHandlers::~TabletServerPathHandlers() {
}

Status TabletServerPathHandlers::Register(Webserver* server) {
  server->RegisterPathHandler(
    "/tablets",
    boost::bind(&TabletServerPathHandlers::HandleTabletsPage, this, _1, _2),
    true /* styled */, true /* is_on_nav_bar */);
  server->RegisterPathHandler(
    "/tablet",
    boost::bind(&TabletServerPathHandlers::HandleTabletPage, this, _1, _2),
    true /* styled */, false /* is_on_nav_bar */);
  server->RegisterPathHandler(
    "/transactionz",
    boost::bind(&TabletServerPathHandlers::HandleTransactionsPage, this, _1, _2),
    true /* styled */, true /* is_on_nav_bar */);

  return Status::OK();
}


void TabletServerPathHandlers::HandleTransactionsPage(const Webserver::ArgumentMap& args,
                                                      std::stringstream* output) {
  vector<shared_ptr<TabletPeer> > peers;
  tserver_->tablet_manager()->GetTabletPeers(&peers);

  *output << "<h1>Transactions</h1>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "   <tr><th>Tablet id</th><th>Op Id</th>"
      "<th>Type</th><th>Total time in-flight</th><th>Description</th></tr>\n";
  BOOST_FOREACH(const shared_ptr<TabletPeer>& peer, peers) {
    vector<TransactionStatusPB> inflight;

    if (peer->tablet() == NULL) {
      continue;
    }

    peer->GetInFlightTransactions(&inflight);
    BOOST_FOREACH(const TransactionStatusPB& inflight_tx, inflight) {
      string total_time_str = Substitute("$0 us.", inflight_tx.running_for_micros());
      (*output) << Substitute("<tr><th>$0</th><th>$1</th><th>$2</th><th>$3</th><th>$4</th></tr>\n",
                              EscapeForHtmlToString(peer->tablet_id()),
                              EscapeForHtmlToString(inflight_tx.op_id().ShortDebugString()),
                              OperationType_Name(inflight_tx.tx_type()),
                              total_time_str,
                              EscapeForHtmlToString(inflight_tx.description()));

    }
  }
  *output << "</table>\n";
}

void TabletServerPathHandlers::HandleTabletsPage(const Webserver::ArgumentMap &args,
                                                 std::stringstream *output) {
  vector<shared_ptr<TabletPeer> > peers;
  tserver_->tablet_manager()->GetTabletPeers(&peers);

  *output << "<h1>Tablets</h1>\n";
  *output << "<table class='table table-striped'>\n";
  *output << "  <tr><th>Table name</th><th>Tablet ID</th>"
      "<th>Start key</th><th>End key</th>"
      "<th>State</th><th>On-disk size</th><th>Last status</th></tr>\n";
  BOOST_FOREACH(const shared_ptr<TabletPeer>& peer, peers) {
    TabletStatusPB status;
    peer->GetTabletStatusPB(&status);
    string id = status.tablet_id();
    string table_name = status.table_name();
    string tablet_id_or_link;
    const Schema& schema = peer->status_listener()->schema();
    if (peer->tablet() != NULL) {
      tablet_id_or_link = Substitute("<a href=\"/tablet?id=$0\">$1</a>",
                                UrlEncodeToString(id),
                                EscapeForHtmlToString(id));
    } else {
      tablet_id_or_link = EscapeForHtmlToString(id);
    }
    string n_bytes = "";
    if (status.has_estimated_on_disk_size()) {
      n_bytes = HumanReadableNumBytes::ToString(status.estimated_on_disk_size());
    }
    string state = metadata::TabletStatePB_Name(status.state());
    if (status.state() == metadata::FAILED) {
      StrAppend(&state, ": ", EscapeForHtmlToString(peer->error().ToString()));
    }
    // TODO: would be nice to include some other stuff like memory usage
    (*output) << Substitute("<tr><th>$0</th><th>$1</th><th>$2</th>"
                            "<th>$3</th><th>$4</th><th>$5</th></tr>\n",
                            EscapeForHtmlToString(table_name),
                            tablet_id_or_link,
                            EscapeForHtmlToString(schema.DebugEncodedRowKey(status.start_key())),
                            EscapeForHtmlToString(schema.DebugEncodedRowKey(status.end_key())),
                            state, n_bytes,
                            EscapeForHtmlToString(status.last_status()));
  }
  *output << "</table>\n";
}

void TabletServerPathHandlers::HandleTabletPage(const Webserver::ArgumentMap &args,
                                                std::stringstream *output) {
  // Parse argument.
  string tablet_id;
  if (!FindCopy(args, "id", &tablet_id)) {
    // TODO: webserver should give a way to return a non-200 response code
    (*output) << "Missing 'id' argument";
    return;
  }

  // Look up tablet.
  shared_ptr<TabletPeer> peer;
  if (!tserver_->tablet_manager()->LookupTablet(tablet_id, &peer)) {
    (*output) << "Tablet " << tablet_id << " not found";
    return;
  }

  // Can't look at bootstrapping tablets.
  if (peer->state() == metadata::BOOTSTRAPPING) {
    (*output) << "Tablet " << tablet_id << " is still bootstrapping";
    return;
  }

  string table_name = peer->tablet()->metadata()->table_name();

  *output << "<h1>Tablet " << EscapeForHtmlToString(tablet_id) << "</h1>\n";

  // Output schema in tabular format.
  *output << "<h2>Schema</h2>\n";
  shared_ptr<Schema> schema(peer->tablet()->schema());
  HtmlOutputSchemaTable(*schema.get(), output);

  *output << "<h2>Impala CREATE TABLE statement</h2>\n";
  HtmlOutputImpalaSchema(table_name, *schema.get(), output);
}

} // namespace tserver
} // namespace kudu
