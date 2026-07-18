@load packages/zeek-kafka

redef Log::default_writer = Log::WRITER_KAFKAWRITER;
redef Kafka::topic_name = "zeek-flows";
redef Kafka::tag_json = T;
redef Kafka::logs_to_send = set(Conn::LOG);
redef Kafka::kafka_conf = table(
    ["metadata.broker.list"] = "backend:9092"
);

redef use_conn_size_analyzer = T;
