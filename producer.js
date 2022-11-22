const { Kafka } = require("kafkajs");

// [2] is the third argument, so node producer.js Milo will extract Milo as the msg.
const msg = process.argv[2];

const run = async () => {
  try {
    // Establish TCP connection
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["localhost:9092"],
    });

    const producer = kafka.producer();
    console.log("Connecting...");
    await producer.connect();
    console.log("Connected!");

    // Send a record
    // Partition 1 if M or less, otherwise 2.
    const partition = msg[0] < "N" ? 0 : 1;
    const result = await producer.send({
      topic: "Users",
      messages: [
        {
          value: msg,
          partition: partition,
        },
      ],
    });

    console.log("Actioned producer successfuly. ", result);
    // NB the position is baseOffset, and the inputs are successfully partitioned.
    await producer.disconnect();
  } catch (err) {
    console.log("There was an error ", err);
  } finally {
    process.exit();
  }
};

run();
