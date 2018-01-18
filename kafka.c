#include <gtk/gtk.h>
#include <string.h>
#include <librdkafka/rdkafka.h>
#include "kafka.h"

static const char *brokers = "localhost:9092";


const struct rd_kafka_metadata *get_kafka_metadata()
{
  const struct rd_kafka_metadata *metadata = NULL;

  rd_kafka_t *rk;
  rd_kafka_conf_t *conf;
  char errstr[512];
  int err;

  conf = rd_kafka_conf_new();

  rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (!rk) {
    fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
    return NULL;
  }

  if (rd_kafka_brokers_add(rk, brokers) == 0) {
    fprintf(stderr, "%% No valid brokers specified\n");
    return NULL;
  }

  err = rd_kafka_metadata(rk, TRUE, NULL, &metadata, 5000);

  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    fprintf(stderr, "%% Failed to acquire metadata: %s\n", rd_kafka_err2str(err));
    return NULL;
  }

  g_print("successfully retrieved metadata\n");

  // rd_kafka_conf_destroy(conf);
  // rd_kafka_destroy(rk);

  return metadata;
}


int metadata_get_brokers_cnt(const struct rd_kafka_metadata *metadata) 
{
  return metadata->broker_cnt;
}


void metadata_get_broker_info(const struct rd_kafka_metadata *metadata, int i, char *buff) 
{
  sprintf(buff, "broker %"PRId32" at %s:%i\n",
    metadata->brokers[i].id,
    metadata->brokers[i].host,
    metadata->brokers[i].port
  );
}


int metadata_get_topics_cnt(const struct rd_kafka_metadata *metadata) 
{
  return metadata->topic_cnt;
}

void metadata_print (const struct rd_kafka_metadata *metadata) 
{
        int i, j, k;

        printf("Metadata for %s (from broker %"PRId32": %s):\n",
               "all topics",
               metadata->orig_broker_id,
               metadata->orig_broker_name);


        /* Iterate brokers */
        printf(" %i brokers:\n", metadata->broker_cnt);
        for (i = 0 ; i < metadata->broker_cnt ; i++)
                printf("  broker %"PRId32" at %s:%i\n",
                       metadata->brokers[i].id,
                       metadata->brokers[i].host,
                       metadata->brokers[i].port);

        /* Iterate topics */
        printf(" %i topics:\n", metadata->topic_cnt);
        for (i = 0 ; i < metadata->topic_cnt ; i++) {
                const struct rd_kafka_metadata_topic *t = &metadata->topics[i];
                printf("  topic \"%s\" with %i partitions:",
                       t->topic,
                       t->partition_cnt);
                if (t->err) {
                        printf(" %s", rd_kafka_err2str(t->err));
                        if (t->err == RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE)
                                printf(" (try again)");
                }
                printf("\n");

                /* Iterate topic's partitions */
                for (j = 0 ; j < t->partition_cnt ; j++) {
                        const struct rd_kafka_metadata_partition *p;
                        p = &t->partitions[j];
                        printf("    partition %"PRId32", "
                               "leader %"PRId32", replicas: ",
                               p->id, p->leader);

                        /* Iterate partition's replicas */
                        for (k = 0 ; k < p->replica_cnt ; k++)
                                printf("%s%"PRId32,
                                       k > 0 ? ",":"", p->replicas[k]);

                        /* Iterate partition's ISRs */
                        printf(", isrs: ");
                        for (k = 0 ; k < p->isr_cnt ; k++)
                                printf("%s%"PRId32,
                                       k > 0 ? ",":"", p->isrs[k]);
                        if (p->err)
                                printf(", %s\n", rd_kafka_err2str(p->err));
                        else
                                printf("\n");
                }
        }
}


int kafka_send(const char *topic, const char *msg)
{
  rd_kafka_t *rk;         /* Producer instance handle */
  rd_kafka_topic_t *rkt;  /* Topic object */
  rd_kafka_conf_t *conf;  /* Temporary configuration object */
  char errstr[512];       /* librdkafka API error reporting buffer */
  char buf[512];          /* Message value temporary buffer */

  g_print ("Trying to send message:\n");
  g_print ("Topic: %s\n", topic);
  g_print ("Message: %s\n", msg);

  conf = rd_kafka_conf_new();

  if (rd_kafka_conf_set(conf, "metadata.broker.list", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "Error: %s\n", errstr);
    return 1;
  }

  rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));

  if (!rk) {
    fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
    return 1;
  }

  rkt = rd_kafka_topic_new(rk, topic, NULL);
    if (!rkt) {
      fprintf(stderr, "%% Failed to create topic object: %s\n", "unknown");
      rd_kafka_destroy(rk);
      return 1;
  }

  if (rd_kafka_produce(
        rkt,                       /* Topic object */
        RD_KAFKA_PARTITION_UA,     /* Use builtin partitioner to select partition*/
        RD_KAFKA_MSG_F_COPY,       /* Make a copy of the payload. */
        (void *) msg, strlen(msg), /* Message payload (value) and length */
        NULL, 0,                   /* Optional key and its length */
        NULL                       /* Message opaque, provided in delivery report callback as msg_opaque. */
      ) == -1) 
  {
    fprintf(stderr, "%% Failed to produce to topic %s: %s\n", rd_kafka_topic_name(rkt), "unknown");
  }

  rd_kafka_flush(rk, 10*1000 /* wait for max 10 seconds */);

  rd_kafka_topic_destroy(rkt);
  rd_kafka_destroy(rk);

  g_print ("Message sent\n");
}
