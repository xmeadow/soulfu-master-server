// SoulFu Master Server
// Implements the ServerFu network protocol for multiplayer matchmaking.
// See wiki/Development: ServerFu.md for protocol specification.
//
// The master server coordinates:
//   - Shard discovery (REQUEST/REPLY_SHARD_LIST)
//   - Version checking (REPLY_VERSION_ERROR)
//   - Player count queries (REQUEST/REPLY_PLAYER_COUNT)
//   - Join coordination (REQUEST_JOIN -> COMMAND_JOIN flow)
//   - IP list management (REQUEST/REPLY_IP_LIST)
//   - Machine tracking (REPORT_MACHINE_DOWN, HEARTBEAT, REPORT_POSITION)
//
// Clients connect via UDP on port 17859.
// Build: cc -o master_server master_server.c -Wall -Wextra
// Usage: ./master_server [port]  (default port: 17859)

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define DEFAULT_PORT        17859
#define MAX_MACHINES        1024    // Maximum machines in a game
#define MAX_SHARDS          26      // 26 shard flags in the spec
#define MAX_PACKET_SIZE     8192
#define PACKET_HEADER_SIZE  3
#define HEARTBEAT_TIMEOUT   180     // 3 minutes in seconds
#define PASSWORD_SIZE       20
#define PASSWORD_OKAY       213     // Magic value from spec

// Packet types from ServerFu spec
#define PACKET_TYPE_REQUEST_SHARD_LIST      10
#define PACKET_TYPE_REPLY_SHARD_LIST        11
#define PACKET_TYPE_REQUEST_PLAYER_COUNT    12
#define PACKET_TYPE_REPLY_PLAYER_COUNT      13
#define PACKET_TYPE_REPLY_VERSION_ERROR     14
#define PACKET_TYPE_REQUEST_JOIN            15
#define PACKET_TYPE_COMMAND_JOIN            16
#define PACKET_TYPE_REPLY_JOIN_OKAY         17
#define PACKET_TYPE_REPLY_ROGER             18
#define PACKET_TYPE_REQUEST_IP_LIST         19
#define PACKET_TYPE_REPLY_IP_LIST           20
#define PACKET_TYPE_REQUEST_ROOM_JOIN       21
#define PACKET_TYPE_REPLY_ROOM_JOIN         22
#define PACKET_TYPE_REQUEST_ALL_ROOM_DATA   23
#define PACKET_TYPE_REPORT_ROOM_CHARACTER   24
#define PACKET_TYPE_REPORT_ROOM_PARTICLE    25
#define PACKET_TYPE_REPORT_ROOM_ORDERS      26
#define PACKET_TYPE_WELCOME_TO_ROOM         27
#define PACKET_TYPE_REPORT_MACHINE_DOWN     28
#define PACKET_TYPE_HEARTBEAT               29
#define PACKET_TYPE_REPORT_POSITION         30

// Required versions - update these as needed
#define REQUIRED_EXE_VERSION    1
#define REQUIRED_DATA_VERSION   1

// Machine entry in the game
typedef struct {
    unsigned int    ip_address;
    unsigned char   continent;
    unsigned char   direction;
    unsigned char   letter;
    unsigned char   password_okay;
    unsigned char   room_x;
    unsigned char   room_y;
    unsigned char   room_z;
    time_t          last_heartbeat;
    int             active;
} machine_t;

static machine_t machines[MAX_MACHINES];
static int num_machines = 0;
static unsigned int game_seed = 0;
static unsigned int sun_time = 0;
static unsigned char game_password[PASSWORD_SIZE];
static int game_has_password = 0;
static int running = 1;

// Packet buffer for building responses
static unsigned char send_buffer[MAX_PACKET_SIZE];
static unsigned short send_length;

static void signal_handler(int sig)
{
    (void)sig;
    running = 0;
}

//-----------------------------------------------------------------------------------------------
// Packet building helpers (big-endian, matching client format)
//-----------------------------------------------------------------------------------------------
static void pkt_begin(unsigned char type)
{
    send_length = PACKET_HEADER_SIZE;
    send_buffer[0] = type;
    send_buffer[1] = 0;  // seed (unused on server)
    send_buffer[2] = 0;  // checksum
}

static void pkt_add_uchar(unsigned char val)
{
    send_buffer[send_length++] = val;
}

static void pkt_add_ushort(unsigned short val)
{
    send_buffer[send_length++] = (unsigned char)(val >> 8);
    send_buffer[send_length++] = (unsigned char)(val);
}

static void pkt_add_uint(unsigned int val)
{
    send_buffer[send_length++] = (unsigned char)(val >> 24);
    send_buffer[send_length++] = (unsigned char)(val >> 16);
    send_buffer[send_length++] = (unsigned char)(val >> 8);
    send_buffer[send_length++] = (unsigned char)(val);
}

// Add a raw IP address (already in network byte order)
static void pkt_add_ip(unsigned int ip_net_order)
{
    memcpy(&send_buffer[send_length], &ip_net_order, 4);
    send_length += 4;
}

static void pkt_end(void)
{
    // Calculate checksum over payload (after header)
    unsigned char checksum = 0;
    for (unsigned short i = PACKET_HEADER_SIZE; i < send_length; i++) {
        checksum += send_buffer[i];
    }
    send_buffer[2] = checksum;
}

//-----------------------------------------------------------------------------------------------
// Packet reading helpers
//-----------------------------------------------------------------------------------------------
static unsigned short read_pos;

static unsigned char pkt_read_uchar(const unsigned char *buf)
{
    return buf[read_pos++];
}

static unsigned short pkt_read_ushort(const unsigned char *buf)
{
    unsigned short val = (buf[read_pos] << 8) | buf[read_pos + 1];
    read_pos += 2;
    return val;
}

static unsigned int pkt_read_ip(const unsigned char *buf)
{
    unsigned int ip;
    memcpy(&ip, &buf[read_pos], 4);
    read_pos += 4;
    return ip;
}

//-----------------------------------------------------------------------------------------------
// Machine management
//-----------------------------------------------------------------------------------------------
static int find_machine_by_ip(unsigned int ip)
{
    for (int i = 0; i < MAX_MACHINES; i++) {
        if (machines[i].active && machines[i].ip_address == ip)
            return i;
    }
    return -1;
}

static int add_machine(unsigned int ip, unsigned char continent, unsigned char direction, unsigned char letter)
{
    int idx = find_machine_by_ip(ip);
    if (idx >= 0) {
        // Update existing
        machines[idx].continent = continent;
        machines[idx].direction = direction;
        machines[idx].letter = letter;
        machines[idx].last_heartbeat = time(NULL);
        return idx;
    }
    for (int i = 0; i < MAX_MACHINES; i++) {
        if (!machines[i].active) {
            machines[i].ip_address = ip;
            machines[i].continent = continent;
            machines[i].direction = direction;
            machines[i].letter = letter;
            machines[i].password_okay = 0;
            machines[i].room_x = 0;
            machines[i].room_y = 0;
            machines[i].room_z = 0;
            machines[i].last_heartbeat = time(NULL);
            machines[i].active = 1;
            num_machines++;
            return i;
        }
    }
    return -1;
}

static void remove_machine(int idx)
{
    if (idx >= 0 && idx < MAX_MACHINES && machines[idx].active) {
        struct in_addr addr;
        addr.s_addr = machines[idx].ip_address;
        printf("[INFO] Machine %s removed (continent=%d dir=%d letter=%d)\n",
               inet_ntoa(addr), machines[idx].continent, machines[idx].direction, machines[idx].letter);
        machines[idx].active = 0;
        num_machines--;
    }
}

// Find a machine already in the game to handle a join request
static int find_available_machine_for_join(void)
{
    // Pick the first active machine
    for (int i = 0; i < MAX_MACHINES; i++) {
        if (machines[i].active)
            return i;
    }
    return -1;
}

//-----------------------------------------------------------------------------------------------
// Send a UDP packet to a specific address
//-----------------------------------------------------------------------------------------------
static void send_udp(int sock, unsigned int ip_net_order, unsigned short port_net_order)
{
    struct sockaddr_in dest;
    memset(&dest, 0, sizeof(dest));
    dest.sin_family = AF_INET;
    dest.sin_addr.s_addr = ip_net_order;
    dest.sin_port = port_net_order;
    sendto(sock, send_buffer, send_length, 0, (struct sockaddr *)&dest, sizeof(dest));
}

//-----------------------------------------------------------------------------------------------
// Expire machines that haven't heartbeated in HEARTBEAT_TIMEOUT seconds
//-----------------------------------------------------------------------------------------------
static void expire_machines(void)
{
    time_t now = time(NULL);
    for (int i = 0; i < MAX_MACHINES; i++) {
        if (machines[i].active && (now - machines[i].last_heartbeat) > HEARTBEAT_TIMEOUT) {
            struct in_addr addr;
            addr.s_addr = machines[i].ip_address;
            printf("[INFO] Machine %s timed out (no heartbeat for %d seconds)\n",
                   inet_ntoa(addr), HEARTBEAT_TIMEOUT);
            remove_machine(i);
        }
    }
}

//-----------------------------------------------------------------------------------------------
// Handle incoming packet
//-----------------------------------------------------------------------------------------------
static void handle_packet(int sock, unsigned char *buf, int len,
                          struct sockaddr_in *from_addr)
{
    unsigned int from_ip = from_addr->sin_addr.s_addr;
    unsigned short from_port = from_addr->sin_port;
    struct in_addr addr;
    addr.s_addr = from_ip;

    if (len < PACKET_HEADER_SIZE) return;

    unsigned char type = buf[0];
    read_pos = PACKET_HEADER_SIZE;

    switch (type) {

    //-------------------------------------------------------------------------------------------
    // SHARD LIST REQUEST
    // Client sends: exe_version(ushort), data_version(ushort), continent(uchar), direction(uchar)
    // Server replies with shard list or version error
    //-------------------------------------------------------------------------------------------
    case PACKET_TYPE_REQUEST_SHARD_LIST: {
        if (len < PACKET_HEADER_SIZE + 6) break;

        unsigned short exe_ver = pkt_read_ushort(buf);
        unsigned short data_ver = pkt_read_ushort(buf);
        unsigned char continent = pkt_read_uchar(buf);
        unsigned char direction = pkt_read_uchar(buf);

        printf("[INFO] Shard list request from %s (exe=%d data=%d continent=%d dir=%d)\n",
               inet_ntoa(addr), exe_ver, data_ver, continent, direction);

        // Version check
        if (exe_ver != REQUIRED_EXE_VERSION || data_ver != REQUIRED_DATA_VERSION) {
            pkt_begin(PACKET_TYPE_REPLY_VERSION_ERROR);
            pkt_add_ushort(REQUIRED_EXE_VERSION);
            pkt_add_ushort(REQUIRED_DATA_VERSION);
            pkt_end();
            send_udp(sock, from_ip, from_port);
            printf("[INFO] Sent version error to %s\n", inet_ntoa(addr));
            break;
        }

        // Build shard valid flags - for now, mark shard 0 as valid if we have any machines
        unsigned int shard_flags = 0;
        if (num_machines > 0) {
            shard_flags |= 1;  // Shard 0 is valid
        }

        pkt_begin(PACKET_TYPE_REPLY_SHARD_LIST);
        pkt_add_uint(shard_flags);
        pkt_add_ip(from_ip);  // IP address of whoever requested

        // For each valid shard, send the map server IP
        // For now, use the first machine's IP as the map server
        for (int s = 0; s < MAX_SHARDS; s++) {
            if (shard_flags & (1u << s)) {
                int m = find_available_machine_for_join();
                if (m >= 0) {
                    pkt_add_ip(machines[m].ip_address);
                } else {
                    // No machine available, send 0
                    pkt_add_uint(0);
                }
            }
        }
        pkt_end();
        send_udp(sock, from_ip, from_port);
        printf("[INFO] Sent shard list to %s (flags=0x%08x)\n", inet_ntoa(addr), shard_flags);
        break;
    }

    //-------------------------------------------------------------------------------------------
    // PLAYER COUNT REQUEST
    // No payload - reply with total number of players
    //-------------------------------------------------------------------------------------------
    case PACKET_TYPE_REQUEST_PLAYER_COUNT: {
        printf("[INFO] Player count request from %s\n", inet_ntoa(addr));
        pkt_begin(PACKET_TYPE_REPLY_PLAYER_COUNT);
        pkt_add_ushort((unsigned short)num_machines);
        pkt_end();
        send_udp(sock, from_ip, from_port);
        break;
    }

    //-------------------------------------------------------------------------------------------
    // JOIN REQUEST
    // Client sends: exe_version(ushort), data_version(ushort), continent(uchar),
    //               direction(uchar), letter(uchar), num_players(uchar),
    //               password(20 bytes, optional)
    //
    // Flow: Server checks version & password, then either:
    //   - If no machines in game: add directly, send REPLY_JOIN_OKAY
    //   - If machines exist: send COMMAND_JOIN to an existing machine to coordinate
    //-------------------------------------------------------------------------------------------
    case PACKET_TYPE_REQUEST_JOIN: {
        if (len < PACKET_HEADER_SIZE + 8) break;

        unsigned short exe_ver = pkt_read_ushort(buf);
        unsigned short data_ver = pkt_read_ushort(buf);
        unsigned char continent = pkt_read_uchar(buf);
        unsigned char direction = pkt_read_uchar(buf);
        unsigned char letter = pkt_read_uchar(buf);
        unsigned char num_players_joining = pkt_read_uchar(buf);

        printf("[INFO] Join request from %s (exe=%d data=%d continent=%d dir=%d letter=%d players=%d)\n",
               inet_ntoa(addr), exe_ver, data_ver, continent, direction, letter, num_players_joining);

        // Version check
        if (exe_ver != REQUIRED_EXE_VERSION || data_ver != REQUIRED_DATA_VERSION) {
            pkt_begin(PACKET_TYPE_REPLY_VERSION_ERROR);
            pkt_add_ushort(REQUIRED_EXE_VERSION);
            pkt_add_ushort(REQUIRED_DATA_VERSION);
            pkt_end();
            send_udp(sock, from_ip, from_port);
            printf("[INFO] Sent version error to %s\n", inet_ntoa(addr));
            break;
        }

        // Password check
        unsigned char pw_okay = PASSWORD_OKAY;  // Default to okay
        if (game_has_password) {
            // Check if packet includes password
            int pw_offset = PACKET_HEADER_SIZE + 8;
            if (len >= pw_offset + PASSWORD_SIZE) {
                if (memcmp(&buf[pw_offset], game_password, PASSWORD_SIZE) != 0) {
                    pw_okay = 0;  // Password mismatch
                    printf("[INFO] Password mismatch from %s\n", inet_ntoa(addr));
                }
            } else {
                pw_okay = 0;  // No password provided but game requires one
                printf("[INFO] No password provided by %s\n", inet_ntoa(addr));
            }
        }

        if (pw_okay != PASSWORD_OKAY) {
            // Don't let them in
            break;
        }

        int existing = find_available_machine_for_join();
        if (existing >= 0) {
            // Send COMMAND_JOIN to an existing machine to handle the join
            // That machine will then forward COMMAND_JOIN to the joiner
            pkt_begin(PACKET_TYPE_COMMAND_JOIN);
            pkt_add_uchar(continent);
            pkt_add_uchar(direction);
            pkt_add_uchar(letter);
            pkt_add_uchar(pw_okay);
            pkt_add_ip(from_ip);  // IP of the joiner
            pkt_end();
            send_udp(sock, machines[existing].ip_address, from_port);

            printf("[INFO] Sent COMMAND_JOIN to existing machine %s to handle joiner %s\n",
                   inet_ntoa((struct in_addr){.s_addr = machines[existing].ip_address}),
                   inet_ntoa(addr));
        } else {
            // No machines in game yet - this is the first player
            // Add them directly and send JOIN_OKAY
            int idx = add_machine(from_ip, continent, direction, letter);
            if (idx >= 0) {
                machines[idx].password_okay = pw_okay;

                // Generate game seed if this is the first machine
                if (game_seed == 0) {
                    game_seed = (unsigned int)time(NULL);
                }

                pkt_begin(PACKET_TYPE_COMMAND_JOIN);
                pkt_add_uchar(continent);
                pkt_add_uchar(direction);
                pkt_add_uchar(letter);
                pkt_add_uchar(pw_okay);
                pkt_add_ip(from_ip);
                pkt_add_uint(game_seed);  // Game seed sent to the joining machine
                pkt_end();
                send_udp(sock, from_ip, from_port);

                // Follow up with JOIN_OKAY
                sun_time++;
                pkt_begin(PACKET_TYPE_REPLY_JOIN_OKAY);
                pkt_add_uint(sun_time);
                pkt_end();
                send_udp(sock, from_ip, from_port);

                printf("[INFO] First machine %s added to game (seed=%u)\n",
                       inet_ntoa(addr), game_seed);
            }
        }
        break;
    }

    //-------------------------------------------------------------------------------------------
    // COMMAND_JOIN (from a machine already in the game, confirming a joiner was handled)
    // Only accepted from main_server or machines already in game
    //-------------------------------------------------------------------------------------------
    case PACKET_TYPE_COMMAND_JOIN: {
        if (len < PACKET_HEADER_SIZE + 8) break;
        if (find_machine_by_ip(from_ip) < 0) {
            printf("[WARN] COMMAND_JOIN from unknown machine %s, ignoring\n", inet_ntoa(addr));
            break;
        }

        unsigned char continent = pkt_read_uchar(buf);
        unsigned char direction = pkt_read_uchar(buf);
        unsigned char letter = pkt_read_uchar(buf);
        unsigned char pw_ok = pkt_read_uchar(buf);
        unsigned int joiner_ip = pkt_read_ip(buf);

        // Add the joiner to our machine list
        int idx = add_machine(joiner_ip, continent, direction, letter);
        if (idx >= 0) {
            machines[idx].password_okay = pw_ok;
        }

        struct in_addr joiner_addr;
        joiner_addr.s_addr = joiner_ip;
        printf("[INFO] Machine %s confirmed join of %s\n", inet_ntoa(addr), inet_ntoa(joiner_addr));

        // Broadcast COMMAND_JOIN to all other machines in the game
        for (int i = 0; i < MAX_MACHINES; i++) {
            if (machines[i].active && machines[i].ip_address != from_ip && machines[i].ip_address != joiner_ip) {
                pkt_begin(PACKET_TYPE_COMMAND_JOIN);
                pkt_add_uchar(continent);
                pkt_add_uchar(direction);
                pkt_add_uchar(letter);
                pkt_add_uchar(pw_ok);
                pkt_add_ip(joiner_ip);
                pkt_end();
                send_udp(sock, machines[i].ip_address, from_port);
            }
        }
        break;
    }

    //-------------------------------------------------------------------------------------------
    // REPLY_ROGER - Acknowledgment from a machine
    //-------------------------------------------------------------------------------------------
    case PACKET_TYPE_REPLY_ROGER: {
        int idx = find_machine_by_ip(from_ip);
        if (idx >= 0) {
            machines[idx].last_heartbeat = time(NULL);
        }
        break;
    }

    //-------------------------------------------------------------------------------------------
    // IP LIST REQUEST
    // Client sends: list_portion(uchar) - 0=IPs 0-63, 1=IPs 64-127, etc.
    //-------------------------------------------------------------------------------------------
    case PACKET_TYPE_REQUEST_IP_LIST: {
        if (len < PACKET_HEADER_SIZE + 1) break;
        unsigned char portion = pkt_read_uchar(buf);

        printf("[INFO] IP list request from %s (portion=%d)\n", inet_ntoa(addr), portion);

        pkt_begin(PACKET_TYPE_REPLY_IP_LIST);
        pkt_add_uchar(portion);

        int start = portion * 64;
        int count = 0;
        for (int i = 0; i < MAX_MACHINES && count < 64; i++) {
            if (machines[i].active) {
                if (start > 0) {
                    start--;
                    continue;
                }
                // Per machine: room x/y/z, password_okay, ip_address
                pkt_add_uchar(machines[i].room_x);
                pkt_add_uchar(machines[i].room_y);
                pkt_add_uchar(machines[i].room_z);
                pkt_add_uchar(machines[i].password_okay);
                pkt_add_ip(machines[i].ip_address);
                count++;
            }
        }
        pkt_end();
        send_udp(sock, from_ip, from_port);
        break;
    }

    //-------------------------------------------------------------------------------------------
    // REPORT_MACHINE_DOWN
    // Fields: continent(uchar), direction(uchar), letter(uchar), ip_address(uint)
    // If ip_address is 0.0.0.0, this is sent to the main server with minutes_in_game(ushort)
    //-------------------------------------------------------------------------------------------
    case PACKET_TYPE_REPORT_MACHINE_DOWN: {
        if (len < PACKET_HEADER_SIZE + 7) break;

        unsigned char continent = pkt_read_uchar(buf);
        unsigned char direction = pkt_read_uchar(buf);
        unsigned char letter = pkt_read_uchar(buf);
        unsigned int down_ip = pkt_read_ip(buf);

        if (down_ip == 0) {
            // Self-report to main server with play time
            unsigned short minutes = 0;
            if (len >= PACKET_HEADER_SIZE + 9) {
                minutes = pkt_read_ushort(buf);
            }
            printf("[INFO] Machine %s self-reported down after %d minutes\n",
                   inet_ntoa(addr), minutes);
            int idx = find_machine_by_ip(from_ip);
            if (idx >= 0) remove_machine(idx);
        } else {
            struct in_addr down_addr;
            down_addr.s_addr = down_ip;
            printf("[INFO] Machine %s reports %s is down (continent=%d dir=%d letter=%d)\n",
                   inet_ntoa(addr), inet_ntoa(down_addr), continent, direction, letter);

            int idx = find_machine_by_ip(down_ip);
            if (idx >= 0) remove_machine(idx);

            // Notify all remaining machines
            for (int i = 0; i < MAX_MACHINES; i++) {
                if (machines[i].active && machines[i].ip_address != from_ip) {
                    pkt_begin(PACKET_TYPE_REPORT_MACHINE_DOWN);
                    pkt_add_uchar(continent);
                    pkt_add_uchar(direction);
                    pkt_add_uchar(letter);
                    pkt_add_ip(down_ip);
                    pkt_end();
                    send_udp(sock, machines[i].ip_address, from_port);
                }
            }
        }
        break;
    }

    //-------------------------------------------------------------------------------------------
    // HEARTBEAT - no payload, just update last_heartbeat time
    // Machines heartbeat the IP directly above them every ~1 minute
    // The server also accepts heartbeats to track liveness
    //-------------------------------------------------------------------------------------------
    case PACKET_TYPE_HEARTBEAT: {
        int idx = find_machine_by_ip(from_ip);
        if (idx >= 0) {
            machines[idx].last_heartbeat = time(NULL);
        } else {
            printf("[INFO] Heartbeat from unknown machine %s\n", inet_ntoa(addr));
        }
        break;
    }

    //-------------------------------------------------------------------------------------------
    // REPORT_POSITION - broadcast, no repeat
    // Fields: room_x(uchar), room_y(uchar), room_z(uchar)
    //-------------------------------------------------------------------------------------------
    case PACKET_TYPE_REPORT_POSITION: {
        if (len < PACKET_HEADER_SIZE + 3) break;

        unsigned char rx = pkt_read_uchar(buf);
        unsigned char ry = pkt_read_uchar(buf);
        unsigned char rz = pkt_read_uchar(buf);

        int idx = find_machine_by_ip(from_ip);
        if (idx >= 0) {
            machines[idx].room_x = rx;
            machines[idx].room_y = ry;
            machines[idx].room_z = rz;
            machines[idx].last_heartbeat = time(NULL);
        }
        break;
    }

    default:
        printf("[WARN] Unknown packet type %d from %s\n", type, inet_ntoa(addr));
        break;
    }
}

//-----------------------------------------------------------------------------------------------
// Main
//-----------------------------------------------------------------------------------------------
int main(int argc, char *argv[])
{
    int sock;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_len;
    unsigned char recv_buf[MAX_PACKET_SIZE];
    int port = DEFAULT_PORT;
    int opt = 1;

    if (argc > 1)
        port = atoi(argv[1]);

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    memset(machines, 0, sizeof(machines));
    memset(game_password, 0, sizeof(game_password));

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Enable broadcast reception
    setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &opt, sizeof(opt));

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close(sock);
        return 1;
    }

    printf("SoulFu Master Server (ServerFu protocol) running on UDP port %d\n", port);
    printf("Tracking up to %d machines, %d shards\n", MAX_MACHINES, MAX_SHARDS);
    printf("Heartbeat timeout: %d seconds\n", HEARTBEAT_TIMEOUT);
    printf("Press Ctrl+C to stop\n\n");

    time_t last_expire = time(NULL);

    while (running) {
        fd_set readfds;
        struct timeval tv;
        FD_ZERO(&readfds);
        FD_SET(sock, &readfds);
        tv.tv_sec = 5;
        tv.tv_usec = 0;

        int ret = select(sock + 1, &readfds, NULL, NULL, &tv);
        if (ret < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }

        // Periodic expiry check
        time_t now = time(NULL);
        if (now - last_expire >= 10) {
            expire_machines();
            last_expire = now;
        }

        if (ret == 0) continue;

        if (FD_ISSET(sock, &readfds)) {
            client_len = sizeof(client_addr);
            int n = recvfrom(sock, recv_buf, MAX_PACKET_SIZE, 0,
                             (struct sockaddr *)&client_addr, &client_len);
            if (n > 0) {
                handle_packet(sock, recv_buf, n, &client_addr);
            }
        }
    }

    printf("\nShutting down... (%d machines were connected)\n", num_machines);
    close(sock);
    return 0;
}
