#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <rdkafka.h>
#include <cjson/cJSON.h>

// Cross-platform sleep macro fallback
#if defined(_WIN32) || defined(_WIN64)
    #include <windows.h>
    #define sleep_ms(ms) Sleep(ms)
#else
    #include <unistd.h>
    #define sleep_ms(ms) usleep((ms) * 1000)
#endif

// Configuration Constants
#define KAFKA_BROKERS "localhost:9092"
#define CONSUMER_GROUP "dynamodb-sink-group"
#define SOURCE_TOPIC "user-updates"
#define DLQ_TOPIC "user-updates-dlq"

// Volatile execution flag controlled by OS signals
static volatile sig_atomic_atomic_t run = 1;

static void stop_signal_handler(int sig) {
    (void)sig; // suppress unused parameter warning
    run = 0;
}

int send_update_to_dynamodb(const char *user_id, const char *status, int login_count) {
    printf("[DynamoDB] Updating user %s -> Status: %s, Increment: %d\n", user_id, status, login_count);
    return 1; 
}

void route_to_dlq(rd_kafka_t *producer, rd_kafka_topic_t *dlq_topic, rd_kafka_message_t *rkmessage, const char *reason) {
    fprintf(stderr, "[DLQ] Routing message to DLQ due to: %s\n", reason);
    
    int err = rd_kafka_produce(
        dlq_topic,
        RD_KAFKA_PARTITION_UA, 
        RD_KAFKA_MSG_F_COPY,   
        rkmessage->payload, rkmessage->len,
        rkmessage->key, rkmessage->key_len,
        NULL
    );

    if (err != 0) {
        fprintf(stderr, "[DLQ] Failed to produce to DLQ: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
    } else {
        printf("[DLQ] Successfully offloaded message to %s\n", DLQ_TOPIC);
    }
    
    rd_kafka_poll(producer, 0);
}

void process_message(rd_kafka_message_t *rkmessage, rd_kafka_t *dlq_producer, rd_kafka_topic_t *dlq_topic) {
    if (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        fprintf(stderr, "[Kafka Error] %s\n", rd_kafka_message_errstr(rkmessage));
        return;
    }

    cJSON *json = cJSON_ParseWithLength((const char *)rkmessage->payload, rkmessage->len);
    if (!json) {
        route_to_dlq(dlq_producer, dlq_topic, rkmessage, "Invalid JSON structure");
        return;
    }

    cJSON *user_id_node = cJSON_GetObjectItemCaseSensitive(json, "user_id");
    cJSON *status_node  = cJSON_GetObjectItemCaseSensitive(json, "status");
    cJSON *count_node   = cJSON_GetObjectItemCaseSensitive(json, "login_count");

    if (!cJSON_IsString(user_id_node) || !cJSON_IsString(status_node) || !cJSON_IsNumber(count_node)) {
        route_to_dlq(dlq_producer, dlq_topic, rkmessage, "Missing or poorly typed schema fields");
        cJSON_Delete(json);
        return;
    }

    int success = send_update_to_dynamodb(user_id_node->valuestring, status_node->valuestring, count_node->valueint);
    if (!success) {
        route_to_dlq(dlq_producer, dlq_topic, rkmessage, "DynamoDB destination connection write rejection");
    }

    cJSON_Delete(json);
}

int main() {
    char errstr[512];

    // Catch kill/interrupt signals for graceful termination across platforms
    signal(SIGINT, stop_signal_handler);
    signal(SIGTERM, stop_signal_handler);

    // --- SETUP KAFKA CONSUMER ---
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", KAFKA_BROKERS, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "group.id", CONSUMER_GROUP, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Configuration error: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return 1;
    }

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
    if (rd_kafka_conf_set(p_conf, "bootstrap.servers", KAFKA_BROKERS, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Producer configuration error: %s\n", errstr);
        rd_kafka_conf_destroy(p_conf);
        return 1;
    }
    
    rd_kafka_t *dlq_producer = rd_kafka_new(RD_KAFKA_PRODUCER, p_conf, errstr, sizeof(errstr));
    if (!dlq_producer) {
        fprintf(stderr, "Failed to create DLQ producer: %s\n", errstr);
        return 1;
    }
    rd_kafka_topic_t *dlq_topic = rd_kafka_topic_new(dlq_producer, DLQ_TOPIC, NULL);

    printf("Connector running natively. Listening on topic '%s'...\n", SOURCE_TOPIC);
    printf("Press Ctrl+C to terminate gracefully.\n");

    // --- MAIN POLL LOOP ---
    while (run) {
        rd_kafka_message_t *msg = rd_kafka_consumer_poll(consumer, 500); // 500ms block
        if (msg) {
            process_message(msg, dlq_producer, dlq_topic);
            rd_kafka_message_destroy(msg);
        }
    }

    printf("\nShutting down connector component...\n");

    // --- CLEANUP ---
    rd_kafka_topic_destroy(dlq_topic);
    rd_kafka_destroy(dlq_producer);
    rd_kafka_topic_partition_list_destroy(topics);
    rd_kafka_consumer_close(consumer);
    rd_kafka_destroy(consumer);

    return 0;
}
