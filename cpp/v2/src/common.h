//
// Created by zero on 2025/5/15.
//

#ifndef ROCKETMQ_CLIENT4CPP_EXAMPLE_COMMON_H_
#define ROCKETMQ_CLIENT4CPP_EXAMPLE_COMMON_H_

#include <stdio.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <memory.h>
#include <unistd.h>
#
#endif


// thread sleep
void t_sleep(unsigned int milliseconds) {
#ifdef _WIN32
	Sleep(milliseconds);
#else
	usleep(milliseconds * 1000);
#endif
}


#endif //ROCKETMQ_CLIENT4CPP_EXAMPLE_COMMON_H_
