/*
 *
 * Compilar: gcc -o broker_udp broker_udp.c
 * Uso:      ./broker_udp
 *
 * Puertos usados:
 *   BROKER_PUB_PORT  (8080): escucha mensajes de los publishers
 *   BROKER_SUB_PORT  (8081): escucha registros/cancelaciones de subscribers
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define BROKER_PUB_PORT  8080
#define BROKER_SUB_PORT  8081
#define BUF_SIZE         1024
#define MAX_SUBS         50
#define MAX_TOPICS       20
#define TOPIC_LEN        64

typedef struct {
    struct sockaddr_in addr;
    char topic[TOPIC_LEN];
    int  active;
} Subscriber;

static Subscriber subs[MAX_SUBS];
static int sub_count = 0;

static int find_sub(struct sockaddr_in *addr, const char *topic) {
    int i;
    for (i = 0; i < MAX_SUBS; i++) {
        if (subs[i].active &&
            subs[i].addr.sin_addr.s_addr == addr->sin_addr.s_addr &&
            subs[i].addr.sin_port == addr->sin_port &&
            strcmp(subs[i].topic, topic) == 0) {
            return i;
        }
    }
    return -1;
}

static int add_sub(struct sockaddr_in *addr, const char *topic) {
    int i;
    if (find_sub(addr, topic) >= 0) {
        printf("[BROKER] Suscriptor %s:%d ya registrado en tema '%s'\n",
               inet_ntoa(addr->sin_addr), ntohs(addr->sin_port), topic);
        return 0;
    }
    for (i = 0; i < MAX_SUBS; i++) {
        if (!subs[i].active) {
            subs[i].addr = *addr;
            subs[i].active = 1;
            strncpy(subs[i].topic, topic, TOPIC_LEN - 1);
            subs[i].topic[TOPIC_LEN - 1] = '\0';
            sub_count++;
            printf("[BROKER] Nuevo suscriptor %s:%d -> tema '%s'\n",
                   inet_ntoa(addr->sin_addr), ntohs(addr->sin_port), topic);
            return 0;
        }
    }
    fprintf(stderr, "[BROKER] Tabla de suscriptores llena.\n");
    return -1;
}

static void remove_sub(struct sockaddr_in *addr, const char *topic) {
    int idx = find_sub(addr, topic);
    if (idx >= 0) {
        subs[idx].active = 0;
        sub_count--;
        printf("[BROKER] Suscriptor %s:%d eliminado del tema '%s'\n",
               inet_ntoa(addr->sin_addr), ntohs(addr->sin_port), topic);
    }
}

static void distribute(int sock, const char *topic, const char *payload) {
    char out_msg[BUF_SIZE];
    int i, sent;

    snprintf(out_msg, sizeof(out_msg), "%s|%s", topic, payload);

    for (i = 0; i < MAX_SUBS; i++) {
        if (subs[i].active && strcmp(subs[i].topic, topic) == 0) {
            sent = sendto(sock, out_msg, strlen(out_msg), 0,
                          (struct sockaddr *)&subs[i].addr,
                          sizeof(subs[i].addr));
            if (sent < 0) {
                perror("[BROKER] sendto subscriber");
            } else {
                printf("[BROKER] Reenviado a %s:%d | %s\n",
                       inet_ntoa(subs[i].addr.sin_addr),
                       ntohs(subs[i].addr.sin_port),
                       out_msg);
            }
        }
    }
}

int main(void) {
    int sock_pub, sock_sub;
    struct sockaddr_in addr_pub, addr_sub, sender;
    socklen_t sender_len;
    char buf[BUF_SIZE];
    int n;
    fd_set read_fds;
    int maxfd;

    memset(subs, 0, sizeof(subs));

    sock_pub = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_pub < 0) {
        perror("socket pub");
        exit(EXIT_FAILURE);
    }

    memset(&addr_pub, 0, sizeof(addr_pub));
    addr_pub.sin_family = AF_INET;
    addr_pub.sin_addr.s_addr = INADDR_ANY;
    addr_pub.sin_port = htons(BROKER_PUB_PORT);

    if (bind(sock_pub, (struct sockaddr *)&addr_pub, sizeof(addr_pub)) < 0) {
        perror("bind pub");
        close(sock_pub);
        exit(EXIT_FAILURE);
    }

    sock_sub = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_sub < 0) {
        perror("socket sub");
        close(sock_pub);
        exit(EXIT_FAILURE);
    }

    memset(&addr_sub, 0, sizeof(addr_sub));
    addr_sub.sin_family = AF_INET;
    addr_sub.sin_addr.s_addr = INADDR_ANY;
    addr_sub.sin_port = htons(BROKER_SUB_PORT);

    if (bind(sock_sub, (struct sockaddr *)&addr_sub, sizeof(addr_sub)) < 0) {
        perror("bind sub");
        close(sock_pub);
        close(sock_sub);
        exit(EXIT_FAILURE);
    }

    printf("=== BROKER UDP ===\n");
    printf("Escuchando publishers en puerto %d\n", BROKER_PUB_PORT);
    printf("Escuchando subscribers en puerto %d\n", BROKER_SUB_PORT);

    maxfd = (sock_pub > sock_sub) ? sock_pub : sock_sub;

    while (1) {
        FD_ZERO(&read_fds);
        FD_SET(sock_pub, &read_fds);
        FD_SET(sock_sub, &read_fds);

        if (select(maxfd + 1, &read_fds, NULL, NULL, NULL) < 0) {
            perror("select");
            break;
        }

        if (FD_ISSET(sock_pub, &read_fds)) {
            memset(buf, 0, sizeof(buf));
            sender_len = sizeof(sender);

            n = recvfrom(sock_pub, buf, sizeof(buf) - 1, 0,
                         (struct sockaddr *)&sender, &sender_len);
            if (n > 0) {
                buf[n] = '\0';

                char *cmd = strtok(buf, "|");
                char *topic = strtok(NULL, "|");
                char *payload = strtok(NULL, "");

                if (cmd && topic && payload && strcmp(cmd, "PUB") == 0) {
                    distribute(sock_pub, topic, payload);
                }
            }
        }

        if (FD_ISSET(sock_sub, &read_fds)) {
            memset(buf, 0, sizeof(buf));
            sender_len = sizeof(sender);

            n = recvfrom(sock_sub, buf, sizeof(buf) - 1, 0,
                         (struct sockaddr *)&sender, &sender_len);
            if (n > 0) {
                buf[n] = '\0';

                char *cmd = strtok(buf, "|");
                char *topic = strtok(NULL, "");

                if (cmd && topic) {
                    if (strcmp(cmd, "SUB") == 0) {
                        add_sub(&sender, topic);
                    } else if (strcmp(cmd, "UNSUB") == 0) {
                        remove_sub(&sender, topic);
                    }
                }
            }
        }
    }

    close(sock_pub);
    close(sock_sub);
    return 0;
}