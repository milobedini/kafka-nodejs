const { Kafka } = require("kafkajs");

const run = async () => {
  try {
    // Establish TCP connection
    const kafka = new Kafka({
      clientId: "myapp",
      brokers: ["localhost:9092"],
    });

    const admin = kafka.admin();
    console.log("Connecting...");
    await admin.connect();
    console.log("Connected!");

    // Create Topics, 2 partitions (A-M, N-Z)

    await admin.createTopics({
      topics: [
        {
          topic: "Users",
          numPartitions: 2,
        },
      ],
    });
    console.log("Created User topic with 2 partitions.");

    await admin.disconnect();
  } catch (err) {
    console.log("There was an error ", err);
  } finally {
    process.exit();
  }
};

run();
