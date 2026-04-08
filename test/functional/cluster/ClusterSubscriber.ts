import ConnectionPool from "../../../lib/cluster/ConnectionPool";
import ClusterSubscriber from "../../../lib/cluster/ClusterSubscriber";
import { EventEmitter } from "events";
import MockServer from "../../helpers/mock_server";
import { expect } from "chai";
import { Cluster } from "../../../lib";

describe("ClusterSubscriber", () => {
  it("cleans up subscribers when selecting a new one", async () => {
    const pool = new ConnectionPool({});
    const subscriber = new ClusterSubscriber(pool, new EventEmitter());

    let rejectSubscribes = false;
    const server = new MockServer(30000, (argv) => {
      if (rejectSubscribes && argv[0] === "subscribe") {
        return new Error("Failed to subscribe");
      }
      return "OK";
    });

    pool.findOrCreate({ host: "127.0.0.1", port: 30000 });

    subscriber.start();
    await subscriber.getInstance().subscribe("foo");
    rejectSubscribes = true;

    subscriber.start();
    await subscriber.getInstance().echo("hello");

    subscriber.start();
    await subscriber.getInstance().echo("hello");

    expect(server.getAllClients()).to.have.lengthOf(1);
    subscriber.stop();
    pool.reset([]);
  });

  it("sets correct connection name when connectionName is set", async () => {
    const pool = new ConnectionPool({ connectionName: "test" });
    const subscriber = new ClusterSubscriber(pool, new EventEmitter());

    const clientNames = [];
    new MockServer(30000, (argv) => {
      if (argv[0] === "client" && argv[1] === "setname") {
        clientNames.push(argv[2]);
      }
    });

    pool.findOrCreate({ host: "127.0.0.1", port: 30000 });

    subscriber.start();
    await subscriber.getInstance().subscribe("foo");
    subscriber.stop();
    pool.reset([]);

    expect(clientNames).to.eql(["ioredis-cluster(subscriber):test"]);
  });

  it("sets correct connection name when connectionName is absent", async () => {
    const pool = new ConnectionPool({});
    const subscriber = new ClusterSubscriber(pool, new EventEmitter());

    const clientNames = [];
    new MockServer(30000, (argv) => {
      if (argv[0] === "client" && argv[1] === "setname") {
        clientNames.push(argv[2]);
      }
    });

    pool.findOrCreate({ host: "127.0.0.1", port: 30000 });

    subscriber.start();
    await subscriber.getInstance().subscribe("foo");
    subscriber.stop();
    pool.reset([]);

    expect(clientNames).to.eql(["ioredis-cluster(subscriber)"]);
  });

  it("refreshes slot cache when subscriber node dies, preventing stale topology", (done) => {
    // End-to-end scenario: 2-node cluster, subscriber's node dies, topology
    // changes on the surviving node. The forceRefresh ensures the cluster
    // proactively learns the new topology and can route commands correctly —
    // without waiting for a command to fail first.

    let slotTable = [
      [0, 8191, ["127.0.0.1", 30001]],
      [8192, 16383, ["127.0.0.1", 30002]],
    ];

    const handler = (argv: string[]) => {
      if (argv[0] === "cluster" && argv[1] === "SLOTS") return slotTable;
      if (argv[0] === "cluster" && argv[1] === "INFO") return "cluster_state:ok";
    };

    const server1 = new MockServer(30001, handler);
    const server2 = new MockServer(30002, handler);

    const cluster = new Cluster([{ host: "127.0.0.1", port: 30001 }]);

    cluster.subscribe("test-channel", () => {
      // After subscribe, the subscriber is connected to one of the servers.
      // Determine which one so we can kill it.
      const onServer1 = !!server1.findClientByName(
        "ioredis-cluster(subscriber)"
      );
      const subscriberServer = onServer1 ? server1 : server2;
      const survivorPort = onServer1 ? 30002 : 30001;

      // Simulate topology change: surviving server takes over all slots
      // (as would happen after a failover in a real Redis cluster).
      slotTable = [[0, 16383, ["127.0.0.1", survivorPort]]];

      // The "refresh" event proves the cluster proactively fetched the new
      // topology via forceRefresh — not reactively from a failed command.
      cluster.once("refresh", () => {
        cluster.get("test-key", (err) => {
          expect(err).to.be.null;
          cluster.disconnect();
          done();
        });
      });

      // Kill the subscriber's server. This triggers:
      // -node / subscriber end → forceRefresh → refreshSlotsCache → CLUSTER SLOTS
      // on surviving server → new topology learned.
      subscriberServer.disconnect();
    });
  });
});
