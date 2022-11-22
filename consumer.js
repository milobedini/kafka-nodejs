const { Kafka } = require("kafkajs");

const run = async () => {
  try {
    // Establish TCP connection
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["localhost:9092"],
    });

    const consumer = kafka.consumer({
      groupId: "test",
    });
    console.log("Connecting...");
    await consumer.connect();
    console.log("Connected!");

    // Don't disconnect the consumer as it wants to keep pulling.
    // Can read from the latest position or from the beginning.
    await consumer.subscribe({
      topic: "Users",
      fromBeginning: true,
    });

    await consumer.run({
      // run a function on each message read.
      eachMessage: async (result) => {
        console.log(
          `RVD Msg ${result.message.value} on partition ${result.partition}`
        );
      },
    });
  } catch (err) {
    console.log("There was an error ", err);
  }
};

// Now when you run the producer with a new write, the consumer will automatically run and log the new entry.
// When you run the consumer another time, the new consumer group is assigned one partition only.
// So currently, my 2nd consumer group is reading A-M and the 1st is reading N-Z.

run();
