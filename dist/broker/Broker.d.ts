import type { Message } from "../types/Message.js";
import { Topic } from "./Topic.js";
export declare class Broker {
    private topics;
    createTopic(name: string, partitionCount: number): Topic;
    getTopics(name: string): Topic;
    publish(topicName: string, key: string | undefined, value: any): Message;
}
//# sourceMappingURL=Broker.d.ts.map