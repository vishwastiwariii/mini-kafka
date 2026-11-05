import { Broker } from "../broker/Broker.js";


export class Producer {
    constructor (
        private broker: Broker
    ) {}

    async send(params : {
        topic: string, 
        key?: string, 
        value: any
    }) {
        const message = this.broker.publish(
            params.topic,
            params.key,
            params.value
        )

        return {
            acknowledged: true, 
            message
        }
    }
}