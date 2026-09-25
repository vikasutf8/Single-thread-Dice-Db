# TCP Echo Server --- Go and Node.js

We built a simple TCP Echo Server in Go and Node.js, using no external
dependencies.

## What it does

1.  Starts listening on a host and port.
2.  Waits continuously for incoming TCP connections.
3.  Accepts multiple clients concurrently.
4.  Handles each client in its own goroutine in Go.
5.  Continuously reads messages from each client.
6.  Sends the same message back to the client.
7.  Closes the connection when the client disconnects.

## Go server flow

``` text
main()
  |
  v
setupFlags()
  |
  v
RunSyncTcpServer()
  |
  v
net.Listen()
  |
  v
Accept loop
  |
  +--> Accept client
  |
  +--> Increment active client count
  |
  +--> Start handleClient(conn) in a goroutine
           |
           v
       Read message
           |
           v
       Echo message back
           |
           v
       Repeat until disconnect
           |
           v
       Close connection and decrement client count
```

## Functions

  -----------------------------------------------------------------------
  Function                            Responsibility
  ----------------------------------- -----------------------------------
  `main()`                            Application entry point

  `setupFlags()`                      Reads the host and port from
                                      command-line flags

  `RunSyncTcpServer()`                Starts the TCP listener and accepts
                                      connections

  `handleClient()`                    Handles one connected client

  `readCmd()`                         Reads a command from the TCP
                                      connection

  `respond()`                         Writes the same command back to the
                                      client
  -----------------------------------------------------------------------

## Run the servers

### Go

``` bash
go run main.go
```

### Node.js

``` bash
node main.js
```

## Test with clients

Start the server in one terminal. In three other terminals, run:

``` bash
nc 127.0.0.1 8080
```

Type a message in any client. The server echoes the same message back.

## Important points

-   The server can handle multiple clients concurrently.
-   Each client has its own read-and-echo loop.
-   In Go, each client is handled by a goroutine.
-   In Node.js, client sockets are handled with events.
-   TCP is a byte stream, so this example uses newline-terminated
    messages.
