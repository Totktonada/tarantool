add_library(
    http_server
    STATIC
    http_server.c
    http_thread.c
    managed_thread.c
)
target_link_libraries(http_server uri)
