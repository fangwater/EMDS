#include <iostream>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>

int main() {
    char hostbuffer[256];
    gethostname(hostbuffer, sizeof(hostbuffer));
    struct hostent *host_entry = gethostbyname(hostbuffer);
    if (host_entry == NULL) {
        std::cerr << "Failed to get host entry for " << hostbuffer << std::endl;
        return 1;
    }
    char *IPbuffer = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0]));
    std::cout << "IP: " << std::string(IPbuffer) << "\n";
    return 0;
}
