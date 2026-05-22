# Find Python package
add_subdirectory(cachelib_memory_allocator)

set(MOONCAKE_STORE_SOURCES
    replica.cpp
    allocator.cpp
    master_service.cpp
    centralized_master_service.cpp
    client_service.cpp
    centralized_client_service.cpp
    p2p_client_service.cpp
    client_metric.cpp
    types.cpp
    master_client.cpp
    centralized_master_client.cpp
    p2p_master_client.cpp
    utils.cpp
    master_metric_manager.cpp
    storage_backend.cpp
    thread_pool.cpp
    etcd_helper.cpp
    ha_helper.cpp
    centralized_segment_manager.cpp
    segment_manager.cpp
    client_meta.cpp
    client_manager.cpp
    centralized_client_manager.cpp
    centralized_client_meta.cpp
    p2p_client_manager.cpp
    p2p_master_service.cpp
    p2p_client_meta.cpp
    p2p_segment_manager.cpp
    transfer_task.cpp
    etcd_helper.cpp
    ha_helper.cpp
    rpc_service.cpp
    centralized_rpc_service.cpp
    p2p_rpc_service.cpp
    offset_allocator.cpp
    posix_file.cpp
    client_buffer.cpp
    real_client.cpp
    dummy_client.cpp
    http_metadata_server.cpp
    file_storage.cpp
    tiered_cache/copier_registry.cpp
    tiered_cache/data_copier.cpp
    tiered_cache/tiered_backend.cpp
    data_manager.cpp
    route_cache.cpp
    client_rpc_service.cpp
    peer_client.cpp
    tiered_cache/tiers/dram_tier.cpp
    tiered_cache/tiers/storage_tier.cpp
    tiered_cache/scheduler/simple_policy.cpp
    tiered_cache/scheduler/lru_policy.cpp
    tiered_cache/scheduler/client_scheduler.cpp
)

set(EXTRA_LIBS "")

if(USE_3FS)
  add_subdirectory(hf3fs)
  list(APPEND MOONCAKE_STORE_SOURCES  ${HF3FS_SOURCES})
  find_library(HF3FS_API_LIB hf3fs_api_shared PATHS /usr/lib NO_DEFAULT_PATH)
  if(NOT HF3FS_API_LIB)
    message(FATAL_ERROR "hf3fs_api_shared library not found in /usr/lib")
  endif()
  set(EXTRA_LIBS ${HF3FS_API_LIB})
endif()

# Ascend Cache Tier support
if(USE_ASCEND_CACHE_TIER)
  list(APPEND MOONCAKE_STORE_SOURCES tiered_cache/tiers/ascend_tier.cpp)
  message(STATUS "Ascend Cache Tier enabled")
endif()

# The cache_allocator library
include_directories(${Python3_INCLUDE_DIRS})
add_library(mooncake_store ${MOONCAKE_STORE_SOURCES})
# Note: transfer_engine is PRIVATE to avoid propagating its dependencies (e.g., vendor-specific hardware)
# to targets that don't need it (e.g., mooncake_master).
# Targets that need transfer_engine should link it explicitly.
target_link_libraries(mooncake_store
    PUBLIC
        cachelib_memory_allocator
        ${ETCD_WRAPPER_LIB}
        glog::glog
        gflags::gflags
        ${EXTRA_LIBS}
    PRIVATE
        transfer_engine
)

# Define USE_ASCEND_DIRECT compile definition if enabled
if(USE_ASCEND_DIRECT)
  target_compile_definitions(mooncake_store PRIVATE USE_ASCEND_DIRECT)
endif()

# Link Ascend ACL library if USE_ASCEND_CACHE_TIER is enabled
if(USE_ASCEND_CACHE_TIER)
  # Try to find Ascend ACL library
  set(ASCEND_SEARCH_PATHS
      $ENV{ASCEND_HOME}/lib64
      /usr/local/Ascend/ascend-toolkit/latest/lib64
      /usr/local/Ascend/nnrt/latest/lib64
  )
  find_library(ASCENDCL_LIB ascendcl PATHS ${ASCEND_SEARCH_PATHS} NO_DEFAULT_PATH)

  if(NOT ASCENDCL_LIB)
    message(WARNING "ascendcl library not found. Ascend cache tier will use fallback mode (no ACL).")
  else()
    message(STATUS "Found ascendcl library: ${ASCENDCL_LIB}")

    # Find ascend_hal library (required by ascend_dump)
    set(ASCEND_HAL_SEARCH_PATHS
        $ENV{ASCEND_HOME}/aarch64-linux/lib64/device/lib64
        $ENV{ASCEND_HOME}/aarch64-linux/devlib
        /usr/local/Ascend/cann-8.5.1/aarch64-linux/lib64/device/lib64
        /usr/local/Ascend/cann-8.5.1/aarch64-linux/devlib
    )
    find_library(ASCEND_HAL_LIB ascend_hal PATHS ${ASCEND_HAL_SEARCH_PATHS} NO_DEFAULT_PATH)
    if(ASCEND_HAL_LIB)
      message(STATUS "Found ascend_hal library: ${ASCEND_HAL_LIB}")
      target_link_libraries(mooncake_store PUBLIC ${ASCEND_HAL_LIB})
    endif()
    target_link_libraries(mooncake_store PUBLIC ${ASCENDCL_LIB})

    # Add include paths for Ascend headers
    set(ASCEND_INCLUDE_PATHS
        $ENV{ASCEND_HOME}/include
        /usr/local/Ascend/ascend-toolkit/latest/include
        /usr/local/Ascend/nnrt/latest/include
    )
    target_include_directories(mooncake_store PUBLIC ${ASCEND_INCLUDE_PATHS})

    # Only define USE_ASCEND_CACHE_TIER when library is found
    target_compile_definitions(mooncake_store PUBLIC USE_ASCEND_CACHE_TIER)
  endif()
endif()
if (STORE_USE_ETCD)
    add_dependencies(mooncake_store build_etcd_wrapper)
endif()

if(URING_LIB AND URING_INCLUDE)
  target_compile_definitions(mooncake_store PUBLIC USE_URING)
  target_include_directories(mooncake_store PRIVATE ${URING_INCLUDE})
endif()

if(BUILD_SHARED_LIBS)
  install(TARGETS mooncake_store DESTINATION lib)
endif()

# Master binary
add_executable(mooncake_master master.cpp)

set(MASTER_EXTRA_INCS)
set(MASTER_EXTRA_LIBS)

if (STORE_USE_JEMALLOC)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(JEMALLOC REQUIRED jemalloc)
    list(APPEND MASTER_EXTRA_INCS ${JEMALLOC_INCLUDE_DIRS})
    list(APPEND MASTER_EXTRA_LIBS ${JEMALLOC_STATIC_LIBRARIES})
endif()

target_include_directories(mooncake_master PRIVATE ${MASTER_EXTRA_INCS})
target_link_libraries(mooncake_master PRIVATE
    mooncake_store
    cachelib_memory_allocator
    pthread
    ibverbs
    mooncake_common
    ${ETCD_WRAPPER_LIB}
    ${MASTER_EXTRA_LIBS}
)

if (STORE_USE_ETCD)

# Define USE_ASCEND_DIRECT for mooncake_master and mooncake_client
if(USE_ASCEND_DIRECT)
  target_compile_definitions(mooncake_master PRIVATE USE_ASCEND_DIRECT)
  target_compile_definitions(mooncake_client PRIVATE USE_ASCEND_DIRECT)
endif()
    add_dependencies(mooncake_master build_etcd_wrapper)
endif()

target_compile_options(mooncake_master PRIVATE -Os)
target_link_options(mooncake_master PRIVATE -Os -s)

# Client server binary
add_executable(mooncake_client real_client_main.cpp)
# Client needs transfer_engine for data transfer operations
target_link_libraries(mooncake_client PRIVATE mooncake_store transfer_engine)
target_compile_options(mooncake_client PRIVATE -Os)
target_link_options(mooncake_client PRIVATE -Os -s)

install(TARGETS mooncake_master mooncake_client DESTINATION bin)
