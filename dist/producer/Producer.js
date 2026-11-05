import { Broker } from "../broker/Broker.js";
export class Producer {
    broker;
    constructor(broker) {
        this.broker = broker;
    }
    async send(params) {
        const message = this.broker.publish(params.topic, params.key, params.value);
        return {
            acknowledged: true,
            message
        };
    }
}
//# sourceMappingURL=Producer.js.map