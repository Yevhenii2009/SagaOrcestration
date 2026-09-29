FROM ubuntu:24.04

RUN apt-get update && apt-get install -y g++ librdkafka-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY main.cpp .

RUN g++ -std=c++17 main.cpp -o delivery-service -lrdkafka++ -lrdkafka

CMD ["./delivery-service"]
