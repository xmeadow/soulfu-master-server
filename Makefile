CC ?= cc
TARGET = master_server

all: $(TARGET)

$(TARGET): master_server.c
	$(CC) -o $@ $< -Wall -Wextra

clean:
	rm -f $(TARGET)

.PHONY: all clean
