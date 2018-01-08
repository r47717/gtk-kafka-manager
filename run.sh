#!/bin/sh

gcc `pkg-config --cflags gtk+-3.0` -o app app.c `pkg-config --libs gtk+-3.0 rdkafka` -lz -lpthread -lrt && \
./app

