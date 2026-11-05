import { Broker } from "../broker/Broker.js";
import { Producer } from "../producer/Producer.js";
import { Consumer } from "./Consumer.js";

const broker = new Broker()

broker.createTopic("orders", 2)

const producer = new Producer(broker)

const consumer = new Consumer(broker)

for(let i = 0; i <= 6; i++) {
    await producer.send({
        topic: "orders",  
        value: {
            orderId: 1
        }
    })
}

consumer.subscribe("orders")

const messages = consumer.poll(6)

for(const message of messages.slice(0,3)) {
    consumer.ack(message)
}

console.log(
    "Commited offsets: ", consumer.getCommittedOffsets()
)

consumer.resetToCommitted()

const redeliveredMessages = consumer.poll(6)

console.log("Redelivered", redeliveredMessages)