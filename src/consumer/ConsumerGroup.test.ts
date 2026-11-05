import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";

import { Broker } from "../broker/Broker.js";
import { ConsumerGroup } from "./ConsumerGroup.js";

describe("ConsumerGroup", () => {
    let broker: Broker;
    let group: ConsumerGroup;

    const publish = (count: number) => {
        for (let i = 1; i <= count; i++) {
            broker.publish("orders", undefined, { orderId: i });
        }
    };

    beforeEach(() => {
        broker = new Broker();
        broker.createTopic("orders", 4);
        group = broker.createConsumerGroup("order-processors", "orders");
    });

    it("validates constructor arguments", () => {
        assert.throws(() => new ConsumerGroup(broker, "", "orders"), /Invalid groupId/);
        assert.throws(() => new ConsumerGroup(broker, "g", " "), /Invalid Topic Name/);
        assert.throws(() => new ConsumerGroup(broker, "g", "nope"), /Topic not found/);
    });

    it("assigns all partitions to a single consumer", () => {
        group.registerConsumer("c1");
        assert.deepEqual([...group.getAssignments()], [["c1", [0, 1, 2, 3]]]);
    });

    it("splits partitions evenly between two consumers", () => {
        group.registerConsumer("consumer-1");
        group.registerConsumer("consumer-2");

        assert.deepEqual(
            [...group.getAssignments()],
            [["consumer-1", [0, 1]], ["consumer-2", [2, 3]]]
        );
    });

    it("gives the remainder to the earliest consumers", () => {
        group.registerConsumer("c1");
        group.registerConsumer("c2");
        group.registerConsumer("c3");

        assert.deepEqual(
            [...group.getAssignments()],
            [["c1", [0, 1]], ["c2", [2]], ["c3", [3]]]
        );
    });

    it("leaves extra consumers idle when there are more consumers than partitions", () => {
        for (const id of ["a", "b", "c", "d", "e"]) group.registerConsumer(id);

        const assignments = group.getAssignments();
        assert.deepEqual(assignments.get("e"), []);
        assert.equal([...assignments.values()].flat().length, 4);
    });

    it("rejects duplicate and invalid consumer ids", () => {
        group.registerConsumer("c1");
        assert.throws(() => group.registerConsumer("c1"), /already Exists/);
        assert.throws(() => group.registerConsumer(" "), /Invalid Consumer Id/);
    });

    it("distributes messages and commits offsets", () => {
        const c1 = group.registerConsumer("consumer-1");
        const c2 = group.registerConsumer("consumer-2");

        publish(10); // P0: 3, P1: 3, P2: 2, P3: 2

        const m1 = c1.poll(100);
        const m2 = c2.poll(100);

        assert.equal(m1.length, 6);
        assert.equal(m2.length, 4);
        assert.ok(m1.every(m => m.partition <= 1));
        assert.ok(m2.every(m => m.partition >= 2));

        for (const m of m1) c1.ack(m);
        for (const m of m2) c2.ack(m);

        assert.deepEqual(
            [...group.getCommittedOffsets()],
            [[0, 3], [1, 3], [2, 2], [3, 2]]
        );
    });

    it("redelivers un-acked messages to the new owner after a rebalance", () => {
        const c1 = group.registerConsumer("c1");
        publish(8); // 2 per partition

        const first = c1.poll(100);
        assert.equal(first.length, 8);

        // ack only partition 0's first message
        c1.ack(first.find(m => m.partition === 0 && m.offset === 0)!);

        const c2 = group.registerConsumer("c2"); // c1: [0,1], c2: [2,3]

        // c1 was reassigned: stale messages from partition 2 can't be acked
        const stale = first.find(m => m.partition === 2)!;
        assert.throws(() => c1.ack(stale), /not assigned/);

        // c2 starts partitions 2 and 3 from their committed offsets (0)
        assert.equal(c2.poll(100).length, 4);

        // c1 resumes from the group's committed offsets: P0 from 1, P1 from 0
        assert.deepEqual(
            c1.poll(100).map(m => [m.partition, m.offset]),
            [[0, 1], [1, 0], [1, 1]]
        );
    });

    it("removes a consumer, revokes it, and hands its partitions over", () => {
        group.registerConsumer("c1");
        const c2 = group.registerConsumer("c2");
        publish(8);

        const polled = c2.poll(100);
        assert.equal(polled.length, 4);

        assert.equal(group.removeConsumer("c2"), true);
        assert.equal(group.removeConsumer("c2"), false);

        assert.deepEqual([...group.getAssignments()], [["c1", [0, 1, 2, 3]]]);
        assert.throws(() => c2.ack(polled[0]!), /not assigned/);
        assert.throws(() => group.getConsumer("c2"), /doesn't exists/);
    });

    it("bumps the generation on every membership change", () => {
        const before = group.getGeneration();
        group.registerConsumer("c1");
        assert.ok(group.getGeneration() > before);

        const mid = group.getGeneration();
        group.removeConsumer("c1");
        assert.ok(group.getGeneration() > mid);
    });

    it("rejects invalid, stale and out-of-range commits directly", () => {
        group.registerConsumer("c1");
        publish(4); // 1 per partition
        const gen = group.getGeneration();

        assert.throws(() => group.commitOffset("ghost", gen, 0, 1), /not part of the group/);
        assert.throws(() => group.commitOffset("c1", gen - 1, 0, 1), /Stale generation/);
        assert.throws(() => group.commitOffset("c1", gen, 0, 99), /exceeds available/);
        assert.throws(() => group.commitOffset("c1", gen, 0, -1), /Invalid offset/);

        group.commitOffset("c1", gen, 0, 1);
        assert.throws(() => group.commitOffset("c1", gen, 0, 0), /Cannot move committed offset back/);
    });

    it("returns copies from getAssignments and getCommittedOffsets", () => {
        group.registerConsumer("c1");

        group.getAssignments().get("c1")!.push(99);
        group.getCommittedOffsets().set(0, 99);

        assert.deepEqual(group.getAssignments().get("c1"), [0, 1, 2, 3]);
        assert.equal(group.getCommittedOffsets().get(0), 0);
    });

    it("close() revokes consumers and blocks further use; is idempotent", () => {
        const c1 = group.registerConsumer("c1");
        publish(4);
        const [m] = c1.poll(1);

        group.close();
        group.close();

        assert.deepEqual([...group.getAssignments()], []);
        assert.throws(() => group.registerConsumer("c2"), /closed/);
        assert.throws(() => c1.ack(m!), /not assigned/);
    });
});

describe("Broker consumer groups", () => {
    let broker: Broker;

    beforeEach(() => {
        broker = new Broker();
        broker.createTopic("orders", 2);
    });

    it("creates, fetches and deletes groups", () => {
        const group = broker.createConsumerGroup("g", "orders");

        assert.equal(broker.getConsumerGroup("g"), group);
        assert.throws(() => broker.createConsumerGroup("g", "orders"), /already exists/);

        assert.equal(broker.deleteConsumerGroup("g"), true);
        assert.equal(broker.deleteConsumerGroup("g"), false);
        assert.throws(() => group.registerConsumer("c"), /closed/);
    });

    it("names the missing group in the error", () => {
        assert.throws(() => broker.getConsumerGroup("ghost"), /not found: ghost/);
    });

    it("closes and removes groups when their topic is deleted", () => {
        broker.createTopic("other", 1);

        const orders = broker.createConsumerGroup("g1", "orders");
        broker.createConsumerGroup("g2", "other");
        orders.registerConsumer("c");

        assert.equal(broker.deleteTopic("orders"), true);

        assert.throws(() => broker.getConsumerGroup("g1"), /not found/);
        assert.throws(() => orders.registerConsumer("d"), /closed/);
        assert.ok(broker.getConsumerGroup("g2")); // unrelated group survives
    });
});
