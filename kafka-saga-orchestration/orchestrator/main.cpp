#include <iostream>
#include <string>

#include <librdkafka/rdkafkacpp.h>

using namespace std;

class SagaOrchestrator
{
private:

    RdKafka::Producer* producer;
    RdKafka::KafkaConsumer* consumer;

    string brokers = "kafka:9092";

    // Topic where services send their results
    string eventTopic = "saga-events";

    // Topic where Orchestrator sends final result to Order
    string orderResultTopic = "order-result";

    // Command topics
    string accountCommandTopic = "account-command";
    string productCommandTopic = "product-command";
    string deliveryCommandTopic = "delivery-command";

    string errorString;

public:

    SagaOrchestrator()
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
            cerr << "[ORCHESTRATOR] Producer error: "
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
            "saga-orchestrator",
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
            cerr << "[ORCHESTRATOR] Consumer error: "
                 << errorString << endl;

            return;
        }

        // Listen for events from services
        RdKafka::ErrorCode result =
            consumer->subscribe({
                eventTopic
            });

        if (result != RdKafka::ERR_NO_ERROR)
        {
            cerr << "[ORCHESTRATOR] Subscribe error: "
                 << RdKafka::err2str(result)
                 << endl;

            return;
        }

        cout << "[ORCHESTRATOR] Connected to Kafka!"
             << endl;
    }

    // =========================
    // Send message
    // =========================

    void sendMessage(
        const string& topic,
        const string& message
    )
    {
        RdKafka::ErrorCode result =
            producer->produce(
                topic,
                RdKafka::Topic::PARTITION_UA,
                RdKafka::Producer::RK_MSG_COPY,
                const_cast<char*>(
                    message.c_str()
                ),
                message.size(),
                nullptr,
                0,
                0,
                nullptr
            );

        if (result == RdKafka::ERR_NO_ERROR)
        {
            cout << "[ORCHESTRATOR] Message sent: "
                 << message << endl;
        }
        else
        {
            cerr << "[ORCHESTRATOR] Failed to send message: "
                 << RdKafka::err2str(result)
                 << endl;
        }

        producer->flush(5000);
    }

    // =========================
    // Handle service event
    // =========================

    void handleEvent(const string& event)
    {
        cout << "[ORCHESTRATOR] Event received: "
             << event << endl;

        // Order was created
        if (event == "ORDER_CREATED")
        {
            cout << "[ORCHESTRATOR] Starting Saga..."
                 << endl;

            sendMessage(
                accountCommandTopic,
                "CHECK_ACCOUNT"
            );
        }

        // Account check completed
        else if (event == "ACCOUNT_CHECKED")
        {
            sendMessage(
                productCommandTopic,
                "CHECK_PRODUCT"
            );
        }

        // Product check completed
        else if (event == "PRODUCT_CHECKED")
        {
            sendMessage(
                deliveryCommandTopic,
                "CHECK_DELIVERY"
            );
        }

        // Delivery succeeded
        else if (event == "DELIVERY_CHECKED")
        {
            cout << "[ORCHESTRATOR] "
                 << "Saga completed successfully!"
                 << endl;

            // Tell Order that the Saga succeeded
            sendMessage(
                orderResultTopic,
                "SAGA_COMPLETED"
            );
        }

        // Delivery failed
        else if (event == "DELIVERY_FAILED")
        {
            cout << "[ORCHESTRATOR] "
                 << "Delivery failed!"
                 << endl;

            cout << "[ORCHESTRATOR] "
                 << "Starting compensation..."
                 << endl;

            sendMessage(
                productCommandTopic,
                "COMPENSATE_PRODUCT"
            );
        }

        // Product compensation completed
        else if (event == "PRODUCT_COMPENSATED")
        {
            sendMessage(
                accountCommandTopic,
                "COMPENSATE_ACCOUNT"
            );
        }

        // Account compensation completed
        else if (event == "ACCOUNT_COMPENSATED")
        {
            cout << "[ORCHESTRATOR] "
                 << "Compensation completed!"
                 << endl;

            cout << "[ORCHESTRATOR] "
                 << "Saga failed."
                 << endl;

            // Tell Order that the Saga failed
            sendMessage(
                orderResultTopic,
                "SAGA_FAILED"
            );
        }
    }

    // =========================
    // Main loop
    // =========================

    void run()
    {
        cout << "[ORCHESTRATOR] "
             << "Waiting for events..."
             << endl;

        while (true)
        {
            RdKafka::Message* message =
                consumer->consume(1000);

            if (message->err() ==
                RdKafka::ERR_NO_ERROR)
            {
                string event(
                    static_cast<const char*>(
                        message->payload()
                    ),
                    message->len()
                );

                handleEvent(event);
            }

            delete message;
        }
    }
};

int main()
{
    cout << "[ORCHESTRATOR] Service started!"
         << endl;

    SagaOrchestrator orchestrator;

    orchestrator.run();

    return 0;
}