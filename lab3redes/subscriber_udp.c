#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define BROKER_SUB_PORT 8081
#define LISTEN_PORT 9000
#define BUF_SIZE 1024
#define MAX_TOPICS 20
#define TOPIC_LEN 64

static int g_sock_reg = -1;
static char g_broker_ip[64];
static char g_topics[MAX_TOPICS][TOPIC_LEN];
static int g_topic_count = 0;
static int g_running = 1;
static int g_listen_port = LISTEN_PORT;

static void send_command(int sock, struct sockaddr_in *broker_addr,
                         const char *cmd, const char *topic) {
    char msg[BUF_SIZE];
    snprintf(msg, sizeof(msg), "%s|%s", cmd, topic);
    sendto(sock, msg, strlen(msg), 0,
           (struct sockaddr *)broker_addr,
           sizeof(*broker_addr));
}

static void handle_signal(int signo) {
    int i;
    struct sockaddr_in broker_addr;

    memset(&broker_addr, 0, sizeof(broker_addr));
    broker_addr.sin_family = AF_INET;
    broker_addr.sin_port = htons(BROKER_SUB_PORT);
    broker_addr.sin_addr.s_addr = inet_addr(g_broker_ip);

    for (i = 0; i < g_topic_count; i++) {
        send_command(g_sock_reg, &broker_addr, "UNSUB", g_topics[i]);
    }

    g_running = 0;
}

int main(int argc, char *argv[]) {
    int sock_listen;
    struct sockaddr_in listen_addr, broker_addr, sender;
    socklen_t sender_len;
    char buf[BUF_SIZE];
    int n, i;

    if (argc < 3) {
        fprintf(stderr, "Uso: %s <ip_broker> <tema1> ...\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    strncpy(g_broker_ip, argv[1], sizeof(g_broker_ip) - 1);

    g_topic_count = argc - 2;
    if (g_topic_count > MAX_TOPICS) g_topic_count = MAX_TOPICS;

    for (i = 0; i < g_topic_count; i++) {
        strncpy(g_topics[i], argv[2 + i], TOPIC_LEN - 1);
        g_topics[i][TOPIC_LEN - 1] = '\0';
    }

    signal(SIGINT, handle_signal);

    g_sock_reg = socket(AF_INET, SOCK_DGRAM, 0);

    memset(&broker_addr, 0, sizeof(broker_addr));
    broker_addr.sin_family = AF_INET;
    broker_addr.sin_port = htons(BROKER_SUB_PORT);
    broker_addr.sin_addr.s_addr = inet_addr(g_broker_ip);

    sock_listen = socket(AF_INET, SOCK_DGRAM, 0);

    memset(&listen_addr, 0, sizeof(listen_addr));
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_addr.s_addr = INADDR_ANY;
    listen_addr.sin_port = htons(g_listen_port);

    bind(sock_listen, (struct sockaddr *)&listen_addr, sizeof(listen_addr));

    for (i = 0; i < g_topic_count; i++) {
        char sub_msg[BUF_SIZE];
        snprintf(sub_msg, sizeof(sub_msg), "SUB|%s", g_topics[i]);
        sendto(sock_listen, sub_msg, strlen(sub_msg), 0,
               (struct sockaddr *)&broker_addr, sizeof(broker_addr));
    }

    while (g_running) {
        sender_len = sizeof(sender);
        n = recvfrom(sock_listen, buf, sizeof(buf) - 1, 0,
                     (struct sockaddr *)&sender, &sender_len);

        if (n < 0) continue;

        buf[n] = '\0';

        char *topic = strtok(buf, "|");
        char *payload = strtok(NULL, "");

        if (topic && payload) {
            printf("[%s] %s\n", topic, payload);
        }
    }

    close(g_sock_reg);
    close(sock_listen);
    return 0;
}