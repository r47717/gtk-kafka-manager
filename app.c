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
  sprintf(buff, "Kafka Manager (ver: %s, brokers: %d, topics: %d)", rd_kafka_version_str(), 
    metadata_get_brokers_cnt(metadata), metadata_get_topics_cnt(metadata));
  gtk_window_set_title(GTK_WINDOW(window), buff);
  /*gtk_window_set_default_size(GTK_WINDOW(window), 400, 300);*/

  grid = gtk_grid_new();
  gtk_container_add(GTK_CONTAINER(window), grid);

  entry_topic_buffer = gtk_entry_buffer_new("", 0);
  entry_topic = gtk_entry_new_with_buffer(entry_topic_buffer);
  entry_msg_buffer = gtk_entry_buffer_new("", 0);
  entry_msg = gtk_entry_new_with_buffer(entry_msg_buffer);
  send_button = gtk_button_new_with_label ("Send message");
  g_signal_connect (send_button, "clicked", G_CALLBACK (on_send_clicked), NULL);

  gtk_grid_attach(GTK_GRID(grid), gtk_label_new("Topic: "),   0, 1, 1, 1);
  gtk_grid_attach(GTK_GRID(grid), entry_topic,                1, 1, 1, 1);
  gtk_grid_attach(GTK_GRID(grid), gtk_label_new("Message: "), 0, 2, 1, 1);
  gtk_grid_attach(GTK_GRID(grid), entry_msg,                  1, 2, 1, 1);
  gtk_grid_attach(GTK_GRID(grid), send_button,                1, 3, 1, 1);

  gtk_widget_set_margin_start(grid, 40);
  gtk_widget_set_margin_end(grid, 40);
  gtk_widget_set_margin_top(grid, 40);
  gtk_widget_set_margin_bottom(grid, 40);

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

