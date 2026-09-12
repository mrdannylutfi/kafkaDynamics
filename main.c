#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rdkafka.h>
#include <cjson/cJSON.h>

// Configuration Constants
#define KAFKA_BROKERS "localhost:9092"
#define CONSUMER_GROUP "dynamodb-sink-group"
#define SOURCE_TOPIC "user-updates"
#define DLQ_TOPIC "user-updates-dlq"

// Mock-up function representing the DynamoDB UpdateItem API execution.
// In pure C, this typically formats a JSON payload and transmits it via libcurl 
// using AWS Signature V4 authentication headers.
int send_update_to_dynamodb(const char *user_id, const char *status, int login_count) {
    printf("[DynamoDB] Updating user %s -> Status: %s, Increment: %d\n", user_id, status, login_count);
    // Real implementation would perform an HTTP POST to DynamoDB endpoint
    return 1; 
}

// Routes corrupted or invalid messages to the Dead Letter Queue
void route_to_dlq(rd_kafka_t *producer, rd_kafka_topic_t *dlq_topic, rd_kafka_message_t *rkmessage, const char *reason) {
    fprintf(stderr, "[DLQ] Routing message to DLQ due to: %s\n", reason);
    
    // Send the original payload straight to the DLQ topic
    int err = rd_kafka_produce(
        dlq_topic,
        RD_KAFKA_PARTITION_UA, // Unassigned partition (let Kafka decide)
        RD_KAFKA_MSG_F_COPY,   // Copy payload safely
        rkmessage->payload, rkmessage->len,
        rkmessage->key, rkmessage->key_len,
        NULL
    );

    if (err != 0) {
        fprintf(stderr, "[DLQ] Failed to produce to DLQ: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
    } else {
        printf("[DLQ] Successfully offloaded message to %s\n", DLQ_TOPIC);
    }
    
    // Poll producer to handle delivery reports inside background threads
    rd_kafka_poll(producer, 0);
}

// Main message routing block
void process_message(rd_kafka_message_t *rkmessage, rd_kafka_t *dlq_producer, rd_kafka_topic_t *dlq_topic) {
    if (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "[Kafka Error] %s\n", rd_kafka_message_errstr(rkmessage));
        return;
    }

    // 1. Attempt to parse JSON
    cJSON *json = cJSON_ParseWithLength((const char *)rkmessage->payload, rkmessage->len);
    if (!json) {
        route_to_dlq(dlq_producer, dlq_topic, rkmessage, "Invalid JSON structure");
        return;
    }

    // 2. Extract keys safely
    cJSON *user_id_node = cJSON_GetObjectItemCaseSensitive(json, "user_id");
    cJSON *status_node  = cJSON_GetObjectItemCaseSensitive(json, "status");
    cJSON *count_node   = cJSON_GetObjectItemCaseSensitive(json, "login_count");

    // 3. Validate schema constraints
    if (!cJSON_IsString(user_id_node) || !cJSON_IsString(status_node) || !cJSON_IsNumber(count_node)) {
        route_to_dlq(dlq_producer, dlq_topic, rkmessage, "Missing or poorly typed schema fields");
        cJSON_Delete(json);
        return;
    }

    // 4. Submit validated values to target database
    int success = send_update_to_dynamodb(user_id_node->valuestring, status_node->valuestring, count_node->valueint);
    if (!success) {
        route_to_dlq(dlq_producer, dlq_topic, rkmessage, "DynamoDB destination connection write rejection");
    }

    cJSON_Delete(json);
}

int main() {
    char errstr[512];

    // --- SETUP KAFKA CONSUMER ---
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", KAFKA_BROKERS, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "group.id", CONSUMER_GROUP, errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr, sizeof(errstr));

    rd_kafka_t *consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!consumer) {
        fprintf(stderr, "Failed to create consumer: %s\n", errstr);
        return 1;
    }

    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, SOURCE_TOPIC, RD_KAFKA_PARTITION_UA);
    rd_kafka_subscribe(consumer, topics);

    // --- SETUP KAFKA DLQ PRODUCER ---
    rd_kafka_conf_t *p_conf = rd_kafka_conf_new();
    rd_kafka_conf_set(p_conf, "bootstrap.servers", KAFKA_BROKERS, errstr, sizeof(errstr));
    
    rd_kafka_t *dlq_producer = rd_kafka_new(RD_KAFKA_PRODUCER, p_conf, errstr, sizeof(errstr));
    if (!dlq_producer) {
        fprintf(stderr, "Failed to create DLQ producer: %s\n", errstr);
        return 1;
    }
    rd_kafka_topic_t *dlq_topic = rd_kafka_topic_new(dlq_producer, DLQ_TOPIC, NULL);

    printf("Connector running. Listening on topic '%s'...\n", SOURCE_TOPIC);

    // --- MAIN LOOP ---
    while (1) {
        rd_kafka_message_t *msg = rd_kafka_consumer_poll(consumer, 1000);
        if (msg) {
            process_message(msg, dlq_producer, dlq_topic);
            rd_kafka_message_destroy(msg);
        }
    }

    // --- CLEANUP ---
    rd_kafka_topic_destroy(dlq_topic);
    rd_kafka_destroy(dlq_producer);
    rd_kafka_topic_partition_list_destroy(topics);
    rd_kafka_consumer_close(consumer);
    rd_kafka_destroy(consumer);

    return 0;
}
