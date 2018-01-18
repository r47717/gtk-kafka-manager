const struct rd_kafka_metadata *get_kafka_metadata();

int metadata_get_brokers_cnt(const struct rd_kafka_metadata *metadata);

void metadata_get_broker_info(const struct rd_kafka_metadata *metadata, int i, char *buff);

int metadata_get_topics_cnt(const struct rd_kafka_metadata *metadata);

void metadata_print (const struct rd_kafka_metadata *metadata);

int kafka_send(const char *topic, const char *msg);


