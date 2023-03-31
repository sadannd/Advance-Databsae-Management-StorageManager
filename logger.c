#include "logger.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

/* 
    Created By :- Shikhar Saraswat
    The purpose of this class is to ease the logging process and generalize the log format.
 */

void logger(const char* tag, const char* message) {
   time_t now;
   time(&now);
   //printf("%s [%s]: %s\n", ctime(&now), tag, message);
}