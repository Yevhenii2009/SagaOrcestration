#include <iostream>
#include <string>

#include <librdkafka/rdkafkacpp.h>

using namespace std;

class ProductService
{
private:

    RdKafka::Producer* producer;
    RdKafka::KafkaConsumer* consumer;

    string brokers = "kafka:9092";

    string commandTopic = "product-command";
    string eventTopic = "saga-events";

    string errorString;

public:

    ProductService()
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
            cerr << "[PRODUCT] Producer error: "
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
            "saga-product",
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
            cerr << "[PRODUCT] Consumer error: "
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
            cerr << "[PRODUCT] Subscribe error: "
                 << RdKafka::err2str(result)
                 << endl;

            return;
        }

        cout << "[PRODUCT] Connected to Kafka!"
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
            cout << "[PRODUCT] Event sent: "
                 << event << endl;
        }
        else
        {
            cerr << "[PRODUCT] Failed to send event: "
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
        cout << "[PRODUCT] Command received: "
             << command << endl;

        if (command == "CHECK_PRODUCT")
        {
            cout << "[PRODUCT] Product check successful!"
                 << endl;

            sendEvent("PRODUCT_CHECKED");
        }

        else if (command == "COMPENSATE_PRODUCT")
        {
            cout << "[PRODUCT] Product compensation completed!"
                 << endl;

            sendEvent("PRODUCT_COMPENSATED");
        }
    }

    // =========================
    // Main loop
    // =========================

    void run()
    {
        cout << "[PRODUCT] Waiting for commands..."
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
    cout << "[PRODUCT] Service started!"
         << endl;

    ProductService product;

    product.run();

    return 0;
}