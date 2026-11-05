import { Broker } from "../broker/Broker.js";
export class Consumer {
    broker;
    topicName;
    partitionId;
    currentOffset = 0;
    constructor(broker, topicName, partitionId) {
        this.broker = broker;
        this.topicName = topicName;
        this.partitionId = partitionId;
    }
    async poll() {
        const topic = this.broker.getTopics(this.topicName);
        const partition = topic.getPartition(this.partitionId);
        const message = partition.read(this.currentOffset);
        return message;
    }
    acknowledge(offset) {
        if (offset !== this.currentOffset) {
            throw new Error("Invalid Acknowledgement");
        }
        this.currentOffset++;
    }
}
//# sourceMappingURL=Consumer.js.map