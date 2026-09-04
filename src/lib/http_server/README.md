# HTTP Server library

## Key features

* Support TLS and HTTP/2 (HTTP/1.0 and HTTP/1.1 are NOT supported).
* TLS and HTTP/2 parsing is performed in separate network threads.
* One server may listen multiple addresses.

## Planned features

* Balancing of a new connection to a network thread.
* Re-balancing of connections to network threads on a disbalance beyond a
  threshold.
* Multiple http server instances.
* Readahead buffer.
* Backpressure (`net_msg_max`).
* [http.server][http.server] adaptor.
* Routing library similar to Golang's [ServeMux][serve_mux].
* Routing to [application threads][app_threads].

[http.server]: https://github.com/tarantool/http
[serve_mux]: https://pkg.go.dev/net/http#ServeMux
[app_threads]: https://github.com/tarantool/tarantool/issues/12206

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


