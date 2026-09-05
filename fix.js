'use strict';

const WebSocket = require('ws');
const net = require('net');

const WS_HOST = '0.0.0.0';
const WS_PORT = 8080;

const STRATUM_HOST = '1miner.net';
const STRATUM_PORT = 5102;


// ============================================================
// WebSocket Server
// ============================================================

const wss = new WebSocket.Server(
    {
        host: WS_HOST,
        port: WS_PORT
    },
    () => {
        console.log(
            `[Proxy] WebSocket listening on ws://localhost:${WS_PORT}`
        );

        console.log(
            `[Proxy] Upstream Stratum: ${STRATUM_HOST}:${STRATUM_PORT}`
        );
    }
);


// ============================================================
// Browser connection
// ============================================================

wss.on('connection', (ws, req) => {

    const peer =
        req.socket.remoteAddress || 'unknown';

    console.log(
        `[Proxy] Browser connected: ${peer}`
    );


    // --------------------------------------------------------
    // TCP socket ke Stratum pool
    // --------------------------------------------------------

    const stratum = new net.Socket();

    stratum.setKeepAlive(
        true,
        10000
    );

    stratum.setNoDelay(
        true
    );


    // --------------------------------------------------------
    // State
    // --------------------------------------------------------

    let poolBuffer = '';
    let browserBuffer = '';

    let poolConnected = false;
    let closed = false;

    const queue = [];


    // ========================================================
    // Browser -> WebSocket
    // ========================================================

    function sendBrowser(line) {

        if (
            ws.readyState !==
            WebSocket.OPEN
        ) {
            return;
        }

        const payload =
            line.endsWith('\n')
                ? line
                : line + '\n';

        ws.send(payload);
    }


    // ========================================================
    // Browser -> Pool
    // ========================================================

    function sendPool(line) {

        if (!line) {
            return;
        }

        const payload =
            line.endsWith('\n')
                ? line
                : line + '\n';


        if (poolConnected) {

            const ok =
                stratum.write(payload);

            if (!ok) {

                console.log(
                    '[Proxy] TCP write buffer penuh.'
                );
            }

        } else {

            queue.push(payload);

            console.log(
                `[Proxy] Pool belum terhubung, message di-queue. Queue=${queue.length}`
            );
        }
    }


    // ========================================================
    // Connect ke Stratum
    // ========================================================

    console.log(
        `[Proxy] Connecting to ${STRATUM_HOST}:${STRATUM_PORT}...`
    );


    stratum.connect(
        STRATUM_PORT,
        STRATUM_HOST,
        () => {

            poolConnected = true;

            console.log(
                `[Proxy] Connected to ${STRATUM_HOST}:${STRATUM_PORT}`
            );


            // Kirim semua pesan yang tertahan
            while (queue.length > 0) {

                const payload =
                    queue.shift();

                console.log(
                    `[Proxy -> Pool queued] ${payload.trim()}`
                );

                stratum.write(payload);
            }
        }
    );


    // ========================================================
    // Browser -> Proxy -> Pool
    // ========================================================

    ws.on('message', data => {

        browserBuffer +=
            data.toString();


        let idx;


        while (
            (idx =
                browserBuffer.indexOf('\n')) !== -1
        ) {

            const line =
                browserBuffer
                    .slice(0, idx)
                    .trim();


            browserBuffer =
                browserBuffer.slice(
                    idx + 1
                );


            if (!line) {
                continue;
            }


            console.log(
                `[Browser -> Pool] ${line}`
            );


            /*
             * IMPORTANT:
             *
             * Jangan mengubah JSON Stratum.
             *
             * mining.subscribe
             * mining.authorize
             * mining.submit
             *
             * semuanya diteruskan apa adanya.
             */

            sendPool(line);
        }
    });


    // ========================================================
    // Pool -> Proxy -> Browser
    // ========================================================

    stratum.on('data', data => {

        poolBuffer +=
            data.toString();


        let idx;


        while (
            (idx =
                poolBuffer.indexOf('\n')) !== -1
        ) {

            const line =
                poolBuffer
                    .slice(0, idx)
                    .trim();


            poolBuffer =
                poolBuffer.slice(
                    idx + 1
                );


            if (!line) {
                continue;
            }


            // ------------------------------------------------
            // Logging saja.
            // TIDAK mengubah pesan.
            // ------------------------------------------------

            try {

                const msg =
                    JSON.parse(line);


                if (
                    msg.method ===
                    'mining.set_difficulty'
                ) {

                    console.log(
                        `[Pool] difficulty=${msg.params?.[0]}`
                    );
                }


                else if (
                    msg.method ===
                    'mining.notify'
                ) {

                    console.log(
                        `[Pool] job=${msg.params?.[0]} cleanJobs=${msg.params?.[8]}`
                    );
                }


                else if (
                    msg.method ===
                    'mining.set_extranonce'
                ) {

                    console.log(
                        `[Pool] set_extranonce=${JSON.stringify(msg.params)}`
                    );
                }


                else if (
                    msg.id !== undefined &&
                    (
                        msg.result !== undefined ||
                        msg.error !== undefined
                    )
                ) {

                    console.log(
                        `[Pool] response id=${msg.id} ` +
                        `result=${JSON.stringify(msg.result)} ` +
                        `error=${JSON.stringify(msg.error)}`
                    );
                }

            } catch (_) {

                console.log(
                    '[Pool] Non-JSON line received'
                );
            }


            // ------------------------------------------------
            // TRANSPARENT BRIDGE
            //
            // Jangan ubah:
            //
            // mining.set_difficulty
            // mining.notify
            // mining.set_extranonce
            // response
            // error
            //
            // Semua diteruskan persis.
            // ------------------------------------------------

            console.log(
                `[Pool -> Browser] ${line}`
            );


            sendBrowser(line);
        }
    });


    // ========================================================
    // Stratum error
    // ========================================================

    stratum.on('error', err => {

        poolConnected = false;


        console.error(
            `[Stratum Error] ${err.message}`
        );


        if (
            ws.readyState ===
            WebSocket.OPEN
        ) {

            sendBrowser(
                JSON.stringify({
                    jsonrpc: '2.0',
                    id: null,
                    result: null,
                    error: [
                        -1,
                        `upstream stratum error: ${err.message}`,
                        null
                    ]
                })
            );
        }
    });


    // ========================================================
    // Stratum closed
    // ========================================================

    stratum.on('close', () => {

        poolConnected = false;


        console.log(
            '[Proxy] Upstream Stratum closed.'
        );


        if (
            !closed &&
            ws.readyState ===
            WebSocket.OPEN
        ) {

            ws.close(
                1011,
                'Stratum connection closed'
            );
        }
    });


    // ========================================================
    // Browser closed
    // ========================================================

    ws.on('close', () => {

        if (closed) {
            return;
        }


        closed = true;


        console.log(
            `[Proxy] Browser disconnected: ${peer}`
        );


        queue.length = 0;


        if (
            !stratum.destroyed
        ) {

            stratum.end();
        }
    });


    // ========================================================
    // WebSocket error
    // ========================================================

    ws.on('error', err => {

        console.error(
            `[WebSocket Error] ${err.message}`
        );
    });
});


// ============================================================
// WebSocket server error
// ============================================================

wss.on('error', err => {

    console.error(
        `[WebSocket Server Error] ${err.message}`
    );
});


// ============================================================
// Graceful shutdown
// ============================================================

function shutdown() {

    console.log(
        '\n[Proxy] Shutting down...'
    );


    wss.close(() => {

        console.log(
            '[Proxy] WebSocket server closed.'
        );

        process.exit(0);
    });
}


process.on(
    'SIGINT',
    shutdown
);


process.on(
    'SIGTERM',
    shutdown
);
