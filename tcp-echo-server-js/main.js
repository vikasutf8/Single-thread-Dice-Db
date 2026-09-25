const net = require("node:net");

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

        // Buffer incomplete TCP messages.
        let buffer = "";

        // Receive data from the client.
        conn.on("data", (data) => {
            buffer += data.toString();

            // Process newline-delimited commands.
            let newlineIndex;

            while ((newlineIndex = buffer.indexOf("\n")) !== -1) {
                const cmd = buffer.slice(0, newlineIndex + 1);
                buffer = buffer.slice(newlineIndex + 1);

                console.log("Received:", JSON.stringify(cmd));

                // Echo the exact command back.
                respond(conn, cmd);
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

// Write the same message back to the client.
function respond(conn, cmd) {
    conn.write(cmd);
}

// Application entry point.
function main() {
    const { host, port } = setupFlags();

    console.log("Starting TCP Echo Server...");

    RunSyncTcpServer(host, port);
}

main();