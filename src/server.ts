import express from "express";

import { Broker } from "./broker/Broker.js";
import { Producer } from "./producer/Producer.js";

const app = express();

app.use(express.json());

const broker = new Broker()

const producer = new Producer(broker)

// Create topic

app.post("/topics", (req, res) => {

  try {

    const { name, partitions } = req.body;

    const topic = broker.createTopic(
      name,
      partitions
    );

    res.json({
      success: true,
      topic: topic.name
    });

  } catch (error: any) {

    res.status(400).json({
      error: error.message
    });

  }

});

// Publish message

app.post("/messages", async (req, res) => {

  try {

    const { topic, key, value } = req.body;

    const result = await producer.send({
      topic,
      key,
      value
    });

    res.json(result);

  } catch (error: any) {

    res.status(400).json({
      error: error.message
    });

  }

});

// Get topic partitions

app.get("/topics/:name", (req, res) => {

  try {

    const topic = broker.getTopic(req.params.name);

    res.json({

      name: topic.name,

      partitions: topic.getPartitions().map(p => ({

        partitionId: p.partitionId,

        messages: p.getMessages(),

        latestOffset: p.getLatestOffset()

      }))

    });

  } catch (error: any) {

    res.status(404).json({
      error: error.message
    });

  }

});

app.listen(3000, () => {

  console.log("MiniKafka running on port 3000");

});