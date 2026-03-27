/*
 * publisher_udp.c
 * Publicador del sistema de noticias deportivas en tiempo real.
 * Envía mensajes al broker UDP asociados a un tema (partido).
 *
 * Compilar: gcc -o publisher_udp publisher_udp.c
 * Uso:      ./publisher_udp <ip_broker> <tema>
 * Ejemplo:  ./publisher_udp 127.0.0.1 "ColombiaVsArgentina"
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

#define BROKER_PUB_PORT 8080
#define BUF_SIZE 1024

int main(int argc, char *argv[]) {
    int sock;
    struct sockaddr_in broker_addr;
    char msg_input[BUF_SIZE];
    char msg_full[BUF_SIZE];
    int bytes_sent;
    int msg_count = 0;

    if (argc < 3) {
        fprintf(stderr, "Uso: %s <ip_broker> <tema>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    const char *broker_ip = argv[1];
    const char *topic = argv[2];

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    memset(&broker_addr, 0, sizeof(broker_addr));
    broker_addr.sin_family = AF_INET;
    broker_addr.sin_port = htons(BROKER_PUB_PORT);
    broker_addr.sin_addr.s_addr = inet_addr(broker_ip);

    if (broker_addr.sin_addr.s_addr == (in_addr_t)-1) {
        fprintf(stderr, "IP inválida\n");
        close(sock);
        exit(EXIT_FAILURE);
    }

    while (1) {
        printf("[MSG #%d] > ", msg_count + 1);
        fflush(stdout);

        if (fgets(msg_input, sizeof(msg_input), stdin) == NULL) break;

        msg_input[strcspn(msg_input, "\n")] = '\0';

        if (strlen(msg_input) == 0) continue;

        snprintf(msg_full, sizeof(msg_full), "PUB|%s|%s", topic, msg_input);

        bytes_sent = sendto(sock, msg_full, strlen(msg_full), 0,
                            (struct sockaddr *)&broker_addr,
                            sizeof(broker_addr));

        if (bytes_sent >= 0) msg_count++;
    }

    close(sock);
    return 0;
}