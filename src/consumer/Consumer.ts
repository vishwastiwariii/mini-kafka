import type { Message } from "../types/Message.js";
import type { Broker } from "../broker/Broker.js";
import type { Topic } from "../broker/Topic.js";
import type { Partition } from "../broker/Partition.js";

export interface GroupAssignment {
    topicName: string;

    assignedPartitionIds: number[];

    initialCommittedOffsets: Record<number, number>;

    // ConsumerGroup provides this callback.
    commitOffset: (
        partitionId: number,
        nextOffset: number
    ) => void;
}

export class Consumer {
    private topic: Topic | null = null;

    // Only partitions assigned by the ConsumerGroup.
    private partitions: Partition[] = [];

    // Next message offset to fetch from each partition.
    private fetchPositions: Map<number, number> = new Map();

    // Local copy of the group's committed offsets.
    private committedOffsets: Map<number, number> = new Map();

    // Tracks messages actually fetched but not yet acknowledged.
    private fetchedOffsets: Map<number, Set<number>> = new Map();

    // Fair polling across assigned partitions.
    private roundRobinCounter = 0;

    // Callback provided by ConsumerGroup.
    private commitOffsetCallback:
        | ((partitionId: number, nextOffset: number) => void)
        | null = null;

    constructor(
        private readonly broker: Broker
    ) {}

    // -----------------------------------------
    // PARTITION ASSIGNMENT
    // -----------------------------------------

    assign(assignment: GroupAssignment): void {

        // 1. Validate topic name.
        if (
            typeof assignment.topicName !== "string" ||
            assignment.topicName.trim().length === 0
        ) {
            throw new Error("Invalid topic name");
        }

        // 2. Resolve topic.
        const topic = this.broker.getTopic(assignment.topicName);

        // 3. Validate partition IDs.
        const partitionIds = assignment.assignedPartitionIds;

        if (!Array.isArray(partitionIds)) {
            throw new Error("Invalid partition assignment");
        }

        const uniquePartitionIds = new Set<number>();

        const assignedPartitions: Partition[] = [];

        const initialOffsets = new Map<number, number>();

        for (const partitionId of partitionIds) {

            if (
                !Number.isInteger(partitionId) ||
                partitionId < 0
            ) {
                throw new Error(`Invalid partition ID: ${partitionId}`);
            }

            if (uniquePartitionIds.has(partitionId)) {
                throw new Error(`Duplicate partition ID: ${partitionId}`);
            }

            uniquePartitionIds.add(partitionId);

            // getPartition throws if partition doesn't exist.
            const partition = topic.getPartition(partitionId);

            assignedPartitions.push(partition);

            // 4. Initialize committed offset.
            const initialOffset =
                assignment.initialCommittedOffsets[partitionId] ?? 0;

            if (
                !Number.isInteger(initialOffset) ||
                initialOffset < 0
            ) {
                throw new Error(
                    `Invalid committed offset for partition ${partitionId}`
                );
            }

            // Offset represents the NEXT message to consume.
            const maximumOffset = partition.getLatestOffset() + 1;

            if (initialOffset > maximumOffset) {
                throw new Error(
                    `Committed offset exceeds available messages in partition ${partitionId}`
                );
            }

            initialOffsets.set(partitionId, initialOffset);
        }

        // 5. Apply assignment only after validation succeeds.
        this.topic = topic;

        this.partitions = assignedPartitions;

        this.fetchPositions.clear();

        this.committedOffsets.clear();

        this.fetchedOffsets.clear();

        this.roundRobinCounter = 0;

        for (const partitionId of uniquePartitionIds) {

            const initialOffset = initialOffsets.get(partitionId)!;

            this.fetchPositions.set(
                partitionId,
                initialOffset
            );

            this.committedOffsets.set(
                partitionId,
                initialOffset
            );

            this.fetchedOffsets.set(
                partitionId,
                new Set<number>()
            );
        }

        this.commitOffsetCallback = assignment.commitOffset;
    }

    // -----------------------------------------
    // POLL MESSAGES
    // -----------------------------------------

    poll(batchSize: number = 10): Message[] {

        if (!this.topic) {
            throw new Error("Consumer has not been assigned a topic");
        }

        if (
            !Number.isInteger(batchSize) ||
            batchSize <= 0
        ) {
            throw new Error("Batch size must be a positive integer");
        }

        const messages: Message[] = [];

        if (this.partitions.length === 0) {
            return messages;
        }

        while (messages.length < batchSize) {

            let messageFound = false;

            // Check partitions in round-robin order.
            for (
                let i = 0;
                i < this.partitions.length;
                i++
            ) {

                const partitionIndex =
                    (this.roundRobinCounter + i) %
                    this.partitions.length;

                const partition = this.partitions[partitionIndex];

                if (!partition) {
                    continue;
                }

                const partitionId = partition.partitionId;

                const currentOffset =
                    this.fetchPositions.get(partitionId);

                if (currentOffset === undefined) {
                    continue;
                }

                const message = partition.read(currentOffset);

                if (message === null) {
                    continue;
                }

                // Add fetched message to batch.
                messages.push(message);

                // Advance fetch position.
                this.fetchPositions.set(
                    partitionId,
                    currentOffset + 1
                );

                // Track that this message was fetched.
                this.fetchedOffsets
                    .get(partitionId)!
                    .add(currentOffset);

                // Next polling attempt starts from next partition.
                this.roundRobinCounter =
                    (partitionIndex + 1) %
                    this.partitions.length;

                messageFound = true;

                break;
            }

            // Stop if no partition has another message.
            if (!messageFound) {
                break;
            }
        }

        return messages;
    }

    // -----------------------------------------
    // ACKNOWLEDGE MESSAGE
    // -----------------------------------------

    ack(message: Message): void {

        if (!this.topic) {
            throw new Error("Consumer has not been assigned");
        }

        if (!this.commitOffsetCallback) {
            throw new Error("Offset commit handler is missing");
        }

        // 1. Validate topic.
        if (message.topic !== this.topic.name) {
            throw new Error("Message belongs to another topic");
        }

        const partitionId = message.partition;

        // 2. Find assigned partition.
        const partition = this.partitions.find(
            p => p.partitionId === partitionId
        );

        if (!partition) {
            throw new Error("Partition is not assigned to this consumer");
        }

        // 3. Validate offset.
        if (
            !Number.isInteger(message.offset) ||
            message.offset < 0
        ) {
            throw new Error("Invalid message offset");
        }

        // 4. Verify original message identity.
        const originalMessage = partition.read(message.offset);

        if (
            originalMessage === null ||
            originalMessage.id !== message.id
        ) {
            throw new Error("Invalid message");
        }

        const committedOffset =
            this.committedOffsets.get(partitionId);

        const fetchedOffsets =
            this.fetchedOffsets.get(partitionId);

        if (
            committedOffset === undefined ||
            !fetchedOffsets
        ) {
            throw new Error("Invalid partition state");
        }

        // 5. Already acknowledged messages are ignored.
        if (message.offset < committedOffset) {
            return;
        }

        // 6. Enforce ordered acknowledgement.
        if (message.offset !== committedOffset) {
            throw new Error(
                "Messages must be acknowledged in offset order"
            );
        }

        // 7. Ensure this consumer actually fetched the message.
        if (!fetchedOffsets.has(message.offset)) {
            throw new Error("Message has not been fetched");
        }

        // 8. Calculate next committed offset.
        const nextOffset = message.offset + 1;

        // 9. Commit through ConsumerGroup.
        // If the callback throws, local state remains unchanged.
        this.commitOffsetCallback(
            partitionId,
            nextOffset
        );

        // 10. Update local state after successful group commit.
        this.committedOffsets.set(
            partitionId,
            nextOffset
        );

        fetchedOffsets.delete(message.offset);
    }

    // -----------------------------------------
    // GET COMMITTED OFFSETS
    // -----------------------------------------

    getCommittedOffsets(): Map<number, number> {
        return new Map(this.committedOffsets);
    }

    // -----------------------------------------
    // RECOVERY
    // -----------------------------------------

    resetToCommitted(): void {

        if (!this.topic) {
            throw new Error("Consumer has not been assigned");
        }

        for (const partition of this.partitions) {

            const partitionId = partition.partitionId;

            const committedOffset =
                this.committedOffsets.get(partitionId);

            if (committedOffset === undefined) {
                continue;
            }

            // Resume from the last committed offset.
            this.fetchPositions.set(
                partitionId,
                committedOffset
            );

            // Previously fetched, unacknowledged messages
            // must be eligible for redelivery.
            this.fetchedOffsets.get(partitionId)?.clear();
        }

        this.roundRobinCounter = 0;
    }
}