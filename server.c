/* Copyright 2023 <> */
#include <stdlib.h>
#include <string.h>

#include "server.h"
#include "utils.h"
#include "LinkedList.h"
#include "HashTable.h"

#define BMAX 101 //nr maxim de buckets uri

server_memory* init_server_memory() {
	server_memory *new_server;
    new_server = malloc(sizeof(server_memory));
	DIE(new_server == NULL, "could not allocate memory\n");

	new_server->ht = ht_create(BMAX, hash_function_key,
							 compare_function_strings);

	return new_server;
}

void server_store(server_memory* server, char* key, char* value) {
	ht_put(server->ht, key, strlen(key) + 1, value, strlen(value) + 1);
}

void server_remove(server_memory* server, char* key) {
	ht_remove_entry(server->ht, key);
}

char* server_retrieve(server_memory* server, char* key) {
	return (char*)ht_get(server->ht, key);
}

void free_server_memory(server_memory* server) {
	ht_free(server->ht);
	free(server);
}