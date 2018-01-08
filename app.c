#include <gtk/gtk.h>
#include <librdkafka/rdkafka.h>

static const char *brokers = "localhost:9092";


static void show_info(GtkWidget *widget, gpointer data)
{
  g_print("Your Kafka version: %s", rd_kafka_version_str());
}


static int metadata_get_brokers_cnt(const struct rd_kafka_metadata *metadata) {
  return metadata->broker_cnt;
}


static void metadata_get_broker_info(const struct rd_kafka_metadata *metadata, int i, char *buff) {
  
  sprintf(buff, "broker %"PRId32" at %s:%i\n",
    metadata->brokers[i].id,
    metadata->brokers[i].host,
    metadata->brokers[i].port
  );
}


static int metadata_get_topics_cnt(const struct rd_kafka_metadata *metadata) {
  return metadata->topic_cnt;
}


static void metadata_print (const struct rd_kafka_metadata *metadata) 
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


static const struct rd_kafka_metadata *get_kafka_metadata()
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


static void activate(GtkApplication *app, gpointer user_data)
{
  struct rd_kafka_metadata *metadata = (struct rd_kafka_metadata *) user_data;
  GtkWidget *window;
  GtkWidget *grid;
  gchar buff[256];

  printf("1");

  if (metadata == NULL) {
    fprintf(stderr, "metadata is NULL");
  }

  window = gtk_application_window_new(app);
  gtk_window_set_title(GTK_WINDOW(window), "Kafka Manager");
  gtk_window_set_default_size(GTK_WINDOW(window), 1200, 800);

  grid = gtk_grid_new();
  gtk_container_add(GTK_CONTAINER(window), grid);

  gtk_grid_attach(GTK_GRID(grid), gtk_label_new("Parameter"), 0, 0, 1, 1);
  gtk_grid_attach(GTK_GRID(grid), gtk_label_new("Value"), 1, 0, 1, 1);
  gtk_grid_attach(GTK_GRID(grid), gtk_label_new("Kafka version: "), 0, 1, 1, 1);
  gtk_grid_attach(GTK_GRID(grid), gtk_label_new(rd_kafka_version_str()), 1, 1, 1, 1);

  if (metadata != NULL) {
    gtk_grid_attach(GTK_GRID(grid), gtk_label_new("Brokers count: "), 0, 2, 1, 1);
    sprintf(buff, "%d", metadata_get_brokers_cnt(metadata));
    gtk_grid_attach(GTK_GRID(grid), gtk_label_new(buff), 1, 2, 1, 1);
  }
  
  if (metadata != NULL) {
    gtk_grid_attach(GTK_GRID(grid), gtk_label_new("Topics count: "), 0, 3, 1, 1);
    sprintf(buff, "%d", metadata_get_topics_cnt(metadata));
    gtk_grid_attach(GTK_GRID(grid), gtk_label_new(buff), 1, 3, 1, 1);
  }

  gtk_widget_show_all(window);
}


int main (int argc, char **argv)
{
  const struct rd_kafka_metadata *metadata = NULL;
  GtkApplication *app;
  int status;

  printf("1");

  metadata = get_kafka_metadata();

  app = gtk_application_new ("org.gtk.example", G_APPLICATION_FLAGS_NONE);
  g_signal_connect (app, "activate", G_CALLBACK (activate), (gpointer) metadata);
  status = g_application_run (G_APPLICATION (app), argc, argv);
  g_object_unref (app);

  if (metadata)
    rd_kafka_metadata_destroy(metadata);

  return status;
}

