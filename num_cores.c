#include <stdio.h>
#include <unistd.h>

int main() {
    long cores = sysconf(_SC_NPROCESSORS_ONLN);
    
    if (cores == -1) {
        // Error occurred - sysconf returns -1 on failure
        fprintf(stderr, "Error: Could not determine number of cores\n");
        return 1;
    }
    
    printf("This machine has %ld cores.\n", cores);
    return 0;
}