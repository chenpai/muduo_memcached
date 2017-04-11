/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "memcached.h"
#include "jenkins_hash.h"
//#include "murmur3_hash.h"
#include "hash.h"
//using namespace muduo;
//using namespace muduo::net;

hash_func hash;
int hash_init(enum hashfunc_type type) {
    switch(type) {
        case JENKINS_HASH:
            {
            hash = jenkins_hash;
            char jens[] = "jenkins";
            settings.hash_algorithm = jens;
            break;
            }
        case MURMUR3_HASH:
//            hash = MurmurHash3_x86_32;
//            settings.hash_algorithm = "murmur3";
            break;
        default:
            return -1;
    }
    return 0;
}
