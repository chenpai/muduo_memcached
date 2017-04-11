if(BOOSTPO_LIBRARY)
  add_executable(memcached_debug  hash.cc assoc.cc bipbuffer.cc  cache.cc daemon.cc  logger.cc items.cc itoa_ljust.cc jenkins_hash.cc  memcached.cc   slabs.cc stats.cc  thread.cc  util.cc )
  target_link_libraries(memcached_debug muduo_net muduo_inspect boost_program_options event)
endif()

add_executable(memcached_muduo  hash.cc assoc.cc bipbuffer.cc  cache.cc daemon.cc  logger.cc items.cc itoa_ljust.cc jenkins_hash.cc  memcached.cc   slabs.cc stats.cc  thread.cc  util.cc )
target_link_libraries(memcached_muduo muduo_net muduo_inspect event)

if(TCMALLOC_INCLUDE_DIR AND TCMALLOC_LIBRARY)
  set_target_properties(memcached_muduo PROPERTIES COMPILE_FLAGS "-DHAVE_TCMALLOC")
  if(BOOSTPO_LIBRARY)
    set_target_properties(memcached_debug PROPERTIES COMPILE_FLAGS "-DHAVE_TCMALLOC")
  endif()
endif()
