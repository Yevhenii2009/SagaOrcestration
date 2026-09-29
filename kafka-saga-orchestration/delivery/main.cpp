#include <iostream>
#include <string>

#include <librdkafka/rdkafkacpp.h>

using namespace std;

class DeliveryService
{
private:

    RdKafka::Producer* producer;
    RdKafka::KafkaConsumer* consumer;

    string brokers = "kafka:9092";

    string commandTopic = "delivery-command";
    string eventTopic = "saga-events";

    string errorString;

public:

    DeliveryService()
    {
        // =========================
        // Create producer
        // =========================

        RdKafka::Conf* producerConfig =
            RdKafka::Conf::create(
                RdKafka::Conf::CONF_GLOBAL
            );

        producerConfig->set(
            "bootstrap.servers",
            brokers,
            errorString
        );

        producer =
            RdKafka::Producer::create(
                producerConfig,
                errorString
            );

        if (!producer)
        {
            cerr << "[DELIVERY] Producer error: "
                 << errorString << endl;

            return;
        }

        // =========================
        // Create consumer
        // =========================

        RdKafka::Conf* consumerConfig =
            RdKafka::Conf::create(
                RdKafka::Conf::CONF_GLOBAL
            );

        consumerConfig->set(
            "bootstrap.servers",
            brokers,
            errorString
        );

        consumerConfig->set(
            "group.id",
            "saga-delivery",
            errorString
        );

        consumerConfig->set(
            "auto.offset.reset",
            "latest",
            errorString
        );

        consumer =
            RdKafka::KafkaConsumer::create(
                consumerConfig,
                errorString
            );

        if (!consumer)
        {
            cerr << "[DELIVERY] Consumer error: "
                 << errorString << endl;

            return;
        }

        // Listen for commands from Orchestrator
        RdKafka::ErrorCode result =
            consumer->subscribe({
                commandTopic
            });

        if (result != RdKafka::ERR_NO_ERROR)
        {
            cerr << "[DELIVERY] Subscribe error: "
                 << RdKafka::err2str(result)
                 << endl;

            return;
        }

        cout << "[DELIVERY] Connected to Kafka!"
             << endl;
    }

    // =========================
    // Send event to Orchestrator
    // =========================

    void sendEvent(const string& event)
    {
        RdKafka::ErrorCode result =
            producer->produce(
                eventTopic,
                RdKafka::Topic::PARTITION_UA,
                RdKafka::Producer::RK_MSG_COPY,
                const_cast<char*>(
                    event.c_str()
                ),
                event.size(),
                nullptr,
                0,
                0,
                nullptr
            );

        if (result == RdKafka::ERR_NO_ERROR)
        {
            cout << "[DELIVERY] Event sent: "
                 << event << endl;
        }
        else
        {
            cerr << "[DELIVERY] Failed to send event: "
                 << RdKafka::err2str(result)
                 << endl;
        }

        producer->flush(5000);
    }

    // =========================
    // Handle command
    // =========================

    void handleCommand(const string& command)
    {
        cout << "[DELIVERY] Command received: "
             << command << endl;

        if (command == "CHECK_DELIVERY")
        {
            cout << "[DELIVERY] "
                 << "Delivery check successful!"
                 << endl;

            sendEvent("DELIVERY_CHECKED");
        }
    }

    // =========================
    // Main loop
    // =========================

    void run()
    {
        cout << "[DELIVERY] Waiting for commands..."
             << endl;

        while (true)
        {
            RdKafka::Message* message =
                consumer->consume(1000);

            if (message->err() ==
                RdKafka::ERR_NO_ERROR)
            {
                string command(
                    static_cast<const char*>(
                        message->payload()
                    ),
                    message->len()
                );

                handleCommand(command);
            }

            delete message;
        }
    }
};

int main()
{
    cout << "[DELIVERY] Service started!"
         << endl;

    DeliveryService delivery;

    delivery.run();

    return 0;
}