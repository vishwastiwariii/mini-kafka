import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";

import { Broker } from "../broker/Broker.js";
import { Producer } from "../producer/Producer.js";
import { Consumer } from "./Consumer.js";

describe("Consumer (standalone assignment)", () => {
    let broker: Broker;
    let consumer: Consumer;
    let committed: Map<number, number>;

    beforeEach(async () => {
        broker = new Broker();
        broker.createTopic("orders", 2);

        const producer = new Producer(broker);

        // 7 messages -> P0 offsets 0..3, P1 offsets 0..2
        for (let i = 0; i < 7; i++) {
            await producer.send({ topic: "orders", value: { orderId: i } });
        }

        committed = new Map();

        consumer = new Consumer(broker);
        consumer.assign({
            topicName: "orders",
            assignedPartitionIds: [0, 1],
            initialCommittedOffsets: {},
            commitOffset: (partitionId, nextOffset) => {
                committed.set(partitionId, nextOffset);
            }
        });
    });

    it("throws if used before assign()", () => {
        const fresh = new Consumer(broker);
        assert.throws(() => fresh.poll(), /not been assigned/);
    });

    it("rejects invalid batch sizes", () => {
        assert.throws(() => consumer.poll(0), /positive integer/);
        assert.throws(() => consumer.poll(1.5), /positive integer/);
    });

    it("validates assignments", () => {
        const c = new Consumer(broker);
        const base = {
            topicName: "orders",
            initialCommittedOffsets: {},
            commitOffset: () => {}
        };

        assert.throws(() => c.assign({ ...base, assignedPartitionIds: [0, 0] }), /Duplicate/);
        assert.throws(() => c.assign({ ...base, assignedPartitionIds: [-1] }), /Invalid partition ID/);
        assert.throws(() => c.assign({ ...base, assignedPartitionIds: [9] }), /No partition exists/);
        assert.throws(
            () => c.assign({ ...base, assignedPartitionIds: [0], initialCommittedOffsets: { 0: 99 } }),
            /exceeds available/
        );
    });

    it("polls round-robin across assigned partitions", () => {
        const got = consumer.poll(4).map(m => [m.partition, m.offset]);
        assert.deepEqual(got, [[0, 0], [1, 0], [0, 1], [1, 1]]);
    });

    it("returns every message once, then an empty batch", () => {
        assert.equal(consumer.poll(100).length, 7);
        assert.equal(consumer.poll(100).length, 0);
    });

    it("acks in order and reports commits through the callback", () => {
        for (const m of consumer.poll(100)) consumer.ack(m);

        assert.deepEqual([...consumer.getCommittedOffsets()], [[0, 4], [1, 3]]);
        assert.deepEqual([...committed], [[0, 4], [1, 3]]);
    });

    it("rejects out-of-order, unfetched and foreign acks; ignores duplicates", () => {
        const msgs = consumer.poll(100);
        const p0 = msgs.filter(m => m.partition === 0);

        assert.throws(() => consumer.ack(p0[1]!), /offset order/);

        consumer.ack(p0[0]!);
        consumer.ack(p0[0]!); // duplicate: no-op
        assert.equal(consumer.getCommittedOffsets().get(0), 1);

        const fresh = new Consumer(broker);
        fresh.assign({
            topicName: "orders",
            assignedPartitionIds: [0],
            initialCommittedOffsets: {},
            commitOffset: () => {}
        });
        assert.throws(() => fresh.ack(p0[0]!), /has not been fetched/);
        assert.throws(() => fresh.ack(msgs.find(m => m.partition === 1)!), /not assigned/);
        assert.throws(() => consumer.ack({ ...p0[0]!, id: "forged" }), /Invalid message/);
    });

    it("leaves local state unchanged if the commit callback throws", () => {
        const c = new Consumer(broker);
        c.assign({
            topicName: "orders",
            assignedPartitionIds: [0],
            initialCommittedOffsets: {},
            commitOffset: () => { throw new Error("revoked"); }
        });

        const [m] = c.poll(1);
        assert.throws(() => c.ack(m!), /revoked/);
        assert.equal(c.getCommittedOffsets().get(0), 0);
    });

    it("redelivers un-acked messages after resetToCommitted", () => {
        const msgs = consumer.poll(6);

        for (const m of msgs.slice(0, 3)) consumer.ack(m);
        assert.deepEqual([...consumer.getCommittedOffsets()], [[0, 2], [1, 1]]);

        consumer.resetToCommitted();

        const redelivered = consumer.poll(100);
        assert.equal(redelivered.length, 4); // 7 published - 3 acked
        assert.ok(redelivered.every(m => m.offset >= consumer.getCommittedOffsets().get(m.partition)!));
    });
});
