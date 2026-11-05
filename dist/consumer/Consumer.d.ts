import { Broker } from "../broker/Broker.js";
import type { Message } from "../types/Message.js";
export declare class Consumer {
    private broker;
    private topicName;
    private partitionId;
    private currentOffset;
    constructor(broker: Broker, topicName: string, partitionId: number);
    poll(): Promise<Message | null>;
    acknowledge(offset: number): void;
}
//# sourceMappingURL=Consumer.d.ts.map