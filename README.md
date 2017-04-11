# Muduo-memcached
1、Overview
----

Memcached is a high-performance distributed memory object cache system for dynamic 
Web applications to reduce Database load. It reduces the number of times the database
is read by caching data and objects in memory Dynamic, database-driven site speed, 
Memcached users have Facebook, Wikipedia,Youtube and so on. 
Memcached is based on the libevent network library for event handling, and libevent 
is not good at handling High throughput situation, and libevent do not have thread 
and socket interface package, It is not easy to use, And it is not conducive to the 
expansion of Memcached. Muduo-memcached to achieve the memcached to C + + changes,
the use of "channel" instead of "event" processing event distribution, Implemented 
memcached cache system based on muduo library.

Thanks for trying Muduo-memcached
----
------------------------------------------------------------------------------------
2、Building Muduo-memcached
----
Muduo-memcached using CMake as build system, the installation method is as follows:
$ sudo apt-get install cmake
Muduo-memcached does not require libevent library, but depends on Boost, the installation method is as follows:
$ sudo apt-get install libboost-dev libboost-test-dev
Muduo-memcached's compilation method is simple:
$ tar zxf muduo_memcached.tar.gz
$ cd muduo_memcached/
$ ./build -j2

------------------------------------------------------------------------------------
3、Running Muduo-memcached(server)
----
$ cd build/release/bin
$ ./muduo_memcached

------------------------------------------------------------------------------------
4、simple Test Muduo-memcached(client)
----
$ telnet 127.0.0.1 11211
$ memcached commend ...

------------------------------------------------------------------------------------
It runs on Linux with kernel version >= 2.6.28.

Copyright (c) 2017, Pai Chen.  


