# HTTP Server library

## Key features

* Supports TLS and HTTP/2 (HTTP/1.0 and HTTP/1.1 are NOT supported).
* IO is served from a separate thread.

## Planned features

* Multiple http server instances.
* Multiple listen sockets.
* Multiple network threads.
* Readahead buffer.
* Backpressure (`net_msg_max`).
* http.server adaptor; XXX: link
* routing library similar to Golang's ServeMux; XXX: link
* routing to application threads: XXX: link

## Libraries

* [openssl][openssl] is used for TLS (via [iostream][iostream]).
* [nghttp2][nghttp2] is used for parsing HTTP/2.
* [libev][libev] is used for non-blocking IO.
* [cbus][cbus] is used for inter-thread communication.

[openssl]: https://openssl-library.org/
[iostream]: https://github.com/tarantool/tarantool/blob/master/src/lib/core/iostream.h
[nghttp2]: https://github.com/nghttp2/nghttp2
[libev]: http://software.schmorp.de/pkg/libev.html
[cbus]: https://github.com/tarantool/tarantool/blob/master/src/lib/core/cbus.h

## Design choices


