#include <iostream>
#include <string>
#include <thread>
#include <chrono>

#include <librdkafka/rdkafkacpp.h>

using namespace std;

class OrderService
{
private:

    RdKafka::Producer* producer;
    RdKafka::KafkaConsumer* consumer;

    string brokers = "kafka:9092";

    string eventTopic = "saga-events";
    string resultTopic = "order-result";

    string errorString;

public:

    OrderService()
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
            cerr << "[ORDER] Producer error: "
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
            "saga-order",
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
            cerr << "[ORDER] Consumer error: "
                 << errorString << endl;

            return;
        }

        // Listen for final Saga result
        RdKafka::ErrorCode result =
            consumer->subscribe({
                resultTopic
            });

        if (result != RdKafka::ERR_NO_ERROR)
        {
            cerr << "[ORDER] Subscribe error: "
                 << RdKafka::err2str(result)
                 << endl;

            return;
        }

        cout << "[ORDER] Connected to Kafka!"
             << endl;
    }

    // =========================
    // Send event
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
            cout << "[ORDER] Event sent: "
                 << event << endl;
        }
        else
        {
            cerr << "[ORDER] Failed to send event: "
                 << RdKafka::err2str(result)
                 << endl;
        }

        producer->flush(5000);
    }

    // =========================
    // Handle final result
    // =========================

    void handleResult(const string& result)
    {
        cout << "[ORDER] Result received: "
             << result << endl;

        if (result == "SAGA_COMPLETED")
        {
            cout << "[ORDER] "
                 << "ORDER COMPLETED!"
                 << endl;
        }

        else if (result == "SAGA_FAILED")
        {
            cout << "[ORDER] "
                 << "ORDER FAILED!"
                 << endl;
        }
    }

    // =========================
    // Start order
    // =========================

    void createOrder()
    {
        cout << "[ORDER] Creating order..."
             << endl;

        sendEvent("ORDER_CREATED");

        cout << "[ORDER] Waiting for Saga result..."
             << endl;
    }

    // =========================
    // Main loop
    // =========================

    void run()
    {
        cout << "[ORDER] Waiting for Kafka..."
             << endl;

        // Give other services time to connect
        this_thread::sleep_for(
            chrono::seconds(5)
        );

        createOrder();

        while (true)
        {
            RdKafka::Message* message =
                consumer->consume(1000);

            if (message->err() ==
                RdKafka::ERR_NO_ERROR)
            {
                string result(
                    static_cast<const char*>(
                        message->payload()
                    ),
                    message->len()
                );

                handleResult(result);

                if (result == "SAGA_COMPLETED" ||
                    result == "SAGA_FAILED")
                {
                    cout << "[ORDER] Saga finished."
                         << endl;

                    break;
                }
            }

            delete message;
        }
    }
};

int main()
{
    cout << "[ORDER] Service started!"
         << endl;

    OrderService order;

    order.run();

    return 0;
}