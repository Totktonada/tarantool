add_library(
    http_server
    STATIC
    http_server.c
    http_thread.c
    http_connection.c
    managed_thread.c
)
target_link_libraries(http_server uri)

# XXX: Remove it, it is just for debugging.
target_compile_definitions(http_server PRIVATE SAY_ENABLED=1)
