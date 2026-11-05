import { Broker } from "../broker/Broker.js";
export declare class Producer {
    private broker;
    constructor(broker: Broker);
    send(params: {
        topic: string;
        key?: string;
        value: any;
    }): Promise<{
        acknowledged: boolean;
        message: import("../types/Message.js").Message;
    }>;
}
//# sourceMappingURL=Producer.d.ts.map