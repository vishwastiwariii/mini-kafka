import type { Message } from "../types/Message.js";
import type { Broker } from "../broker/Broker.js";
import type { Topic } from "../broker/Topic.js";
import type { Partition } from "../broker/Partition.js";

/**
 * Pull-based consumer that subscribes to every partition of a topic.
 *
 * - poll() fetches messages round-robin across partitions and advances the
 *   fetch position.
 * - ack() commits offsets (in order, per partition).
 * - resetToCommitted() rewinds fetch positions to the last committed offsets,
 *   simulating recovery after a crash (un-acked messages are redelivered).
 */
export class Consumer {
    private topic: Topic | null = null;

    private partitions: Partition[] = [];

    // Next message to fetch from each partition
    private fetchPositions: Map<number, number> = new Map();

    // Next message to consume after recovery
    private committedOffsets: Map<number, number> = new Map();

    // Used for fair polling across partitions
    private roundRobinCounter = 0;

    constructor(
        private readonly broker: Broker
    ) {}

    // Subscribe to a topic
    subscribe(topicName: string): void {
        const topic = this.broker.getTopic(topicName);

        this.topic = topic;
        this.partitions = topic.getPartitions();

        this.fetchPositions.clear();
        this.committedOffsets.clear();
        this.roundRobinCounter = 0;

        for (const partition of this.partitions) {
            this.fetchPositions.set(partition.partitionId, 0);
            this.committedOffsets.set(partition.partitionId, 0);
        }
    }

    // Fetch messages from subscribed partitions
    poll(batchSize: number = 10): Message[] {
        if (!this.topic) {
            throw new Error("Consumer is not subscribed to any topic");
        }

        if (!Number.isInteger(batchSize) || batchSize <= 0) {
            throw new Error("Batch size must be a positive integer");
        }

        const messages: Message[] = [];

        if (this.partitions.length === 0) {
            return messages;
        }

        while (messages.length < batchSize) {
            let messageFound = false;

            // Check partitions in round-robin order
            for (let i = 0; i < this.partitions.length; i++) {
                const partitionIndex =
                    (this.roundRobinCounter + i) % this.partitions.length;

                const partition = this.partitions[partitionIndex];

                if (!partition) {
                    continue;
                }

                const partitionId = partition.partitionId;

                const currentOffset = this.fetchPositions.get(partitionId);

                if (currentOffset === undefined) {
                    continue;
                }

                const message = partition.read(currentOffset);

                if (message === null) {
                    continue;
                }

                messages.push(message);

                // Move fetch position forward
                this.fetchPositions.set(partitionId, currentOffset + 1);

                // Start next fetch from the next partition
                this.roundRobinCounter =
                    (partitionIndex + 1) % this.partitions.length;

                messageFound = true;

                break;
            }

            // No partition had another message
            if (!messageFound) {
                break;
            }
        }

        return messages;
    }

    // Acknowledge successful message processing
    ack(message: Message): void {
        if (!this.topic) {
            throw new Error("Consumer is not subscribed");
        }

        // Ensure message belongs to subscribed topic
        if (message.topic !== this.topic.name) {
            throw new Error("Message belongs to another topic");
        }

        const partitionId = message.partition;

        const committedOffset = this.committedOffsets.get(partitionId);

        const fetchPosition = this.fetchPositions.get(partitionId);

        if (committedOffset === undefined || fetchPosition === undefined) {
            throw new Error("Invalid partition");
        }

        if (!Number.isInteger(message.offset) || message.offset < 0) {
            throw new Error("Invalid message offset");
        }

        // Already acknowledged message
        if (message.offset < committedOffset) {
            return;
        }

        // Prevent acknowledging messages out of order
        if (message.offset > committedOffset) {
            throw new Error("Messages must be acknowledged in offset order");
        }

        // Ensure this message was actually fetched
        if (message.offset >= fetchPosition) {
            throw new Error("Message has not been fetched");
        }

        // Commit the next offset
        this.committedOffsets.set(partitionId, message.offset + 1);
    }

    // Return committed offsets without exposing internal state
    getCommittedOffsets(): Map<number, number> {
        return new Map(this.committedOffsets);
    }

    // Simulate consumer recovery after failure
    resetToCommitted(): void {
        if (!this.topic) {
            throw new Error("Consumer is not subscribed");
        }

        for (const partition of this.partitions) {
            const committedOffset = this.committedOffsets.get(
                partition.partitionId
            );

            if (committedOffset === undefined) {
                continue;
            }

            this.fetchPositions.set(partition.partitionId, committedOffset);
        }

        this.roundRobinCounter = 0;
    }
}
