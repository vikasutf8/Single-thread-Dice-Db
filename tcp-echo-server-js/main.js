const net = require("node:net");
const { readCmd } = require("./redis-cmd");
const { evalAndRespond, respondError } = require("./eval");

// Active client connection count.
let conClient = 0;

// Read command-line flags.
function setupFlags() {
    const args = process.argv.slice(2);

    let host = "127.0.0.1";
    let port = 8080;

    for (let i = 0; i < args.length; i++) {
        if (args[i] === "--host") {
            host = args[++i];
        } else if (args[i] === "--port") {
            port = Number(args[++i]);
        }
    }

    return { host, port };
}

// Start the TCP server.
function RunSyncTcpServer(host, port) {
    const server = net.createServer((conn) => {
        // A new client has connected.
        conClient++;

        console.log(
            `New client connected: ${conn.remoteAddress}:${conn.remotePort}` +
            ` | Active clients: ${conClient}`
        );

        // Buffer incomplete TCP messages (RESP bytes, not text lines).
        let leftover = Buffer.alloc(0);

        // Receive data from the client.
        // Infinite read loop: drain every complete RedisCmd in the buffer.
        conn.on("data", (chunk) => {
            leftover = Buffer.concat([leftover, chunk]);

            while (leftover.length > 0) {
                const { cmd, n, err } = readCmd(leftover);

                if (err) {
                    // Incomplete: wait for more bytes.
                    if (err.message.startsWith("incomplete")) break;
                    // Bare Enter in nc: just consume, no reply.
                    if (err.message === "empty line") {
                        leftover = leftover.slice(n);
                        if (leftover.length === 0) break;
                        continue;
                    }
                    // Bad shape / protocol error: reply error, drop bad bytes.
                    console.log("readCmd error:", err.message);
                    respondError(err, conn);
                    leftover = n > 0 ? leftover.slice(n) : Buffer.alloc(0);
                    if (leftover.length === 0) break;
                    continue;
                }

                console.log("Received cmd:", cmd.Cmd, JSON.stringify(cmd.Args));

                // Dispatch: PING -> evalPING, else evalDefault.
                evalAndRespond(cmd, conn);
                leftover = leftover.slice(n);
            }
        });

        // Handle client disconnection.
        conn.on("close", () => {
            conClient--;

            console.log(
                `Client disconnected | Active clients: ${conClient}`
            );
        });

        // Handle socket errors.
        conn.on("error", (err) => {
            console.error("Client socket error:", err.message);
        });
    });

    // Handle server errors.
    server.on("error", (err) => {
        console.error("Server error:", err.message);
    });

    // Bind to the requested host and port.
    server.listen(port, host, () => {
        console.log(`TCP server listening on ${host}:${port}`);
        console.log("Waiting for client connections...");
    });
}

// Application entry point.
function main() {
    const { host, port } = setupFlags();

    console.log("Starting Redis-like Server (PING support)...");

    RunSyncTcpServer(host, port);
}

main();