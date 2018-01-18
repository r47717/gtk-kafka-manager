#include <gtk/gtk.h>
#include <librdkafka/rdkafka.h>
#include "kafka.h"


/* Widgets */

GtkWidget *window;
GtkWidget *grid;
GtkWidget *send_button;
GtkWidget *entry_topic;
GtkEntryBuffer *entry_topic_buffer;
GtkWidget *entry_msg;
GtkEntryBuffer *entry_msg_buffer;


/* Event handlers */

static void on_send_clicked(GtkWidget *widget, gpointer data)
{
  const char *topic = (const char *) gtk_entry_get_text((GtkEntry *)entry_topic);
  const char *msg = (const char *) gtk_entry_get_text((GtkEntry *)entry_msg);
  kafka_send(topic, msg);
}


/* GUI activation */

static void activate(GtkApplication *app, gpointer user_data)
{
  struct rd_kafka_metadata *metadata = (struct rd_kafka_metadata *) user_data;
  gchar buff[256];

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

  entry_topic_buffer = gtk_entry_buffer_new("", 0);
  entry_topic = gtk_entry_new_with_buffer(entry_topic_buffer);
  entry_msg_buffer = gtk_entry_buffer_new("", 0);
  entry_msg = gtk_entry_new_with_buffer(entry_msg_buffer);
  send_button = gtk_button_new_with_label ("Send message");
  g_signal_connect (send_button, "clicked", G_CALLBACK (on_send_clicked), NULL);

  gtk_grid_attach(GTK_GRID(grid), gtk_label_new("Topic: "), 0, 5, 1, 1);
  gtk_grid_attach(GTK_GRID(grid), entry_topic, 1, 5, 1, 1);
  gtk_grid_attach(GTK_GRID(grid), gtk_label_new("Message: "), 0, 6, 1, 1);
  gtk_grid_attach(GTK_GRID(grid), entry_msg, 1, 6, 1, 1);
  gtk_grid_attach(GTK_GRID(grid), send_button, 1, 7, 1, 1);

  gtk_widget_show_all(window);
}


/* Main */

int main (int argc, char **argv)
{
  const struct rd_kafka_metadata *metadata = NULL;
  GtkApplication *app;
  int status;

  metadata = get_kafka_metadata();

  app = gtk_application_new ("org.gtk.example", G_APPLICATION_FLAGS_NONE);
  g_signal_connect (app, "activate", G_CALLBACK (activate), (gpointer) metadata);
  status = g_application_run (G_APPLICATION (app), argc, argv);
  g_object_unref (app);

  if (metadata)
    rd_kafka_metadata_destroy(metadata);

  return status;
}

