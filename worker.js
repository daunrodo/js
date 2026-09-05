import { connect } from 'cloudflare:sockets';

const STRATUM_HOST = '1miner.net';
const STRATUM_PORT = 5102;

export default {
  async fetch(request, env, ctx) {
    // 1. Verifikasi koneksi WebSocket dari Browser
    const upgradeHeader = request.headers.get('Upgrade');
    if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
      return new Response('RDK03 Stratum Proxy Active. Please connect via WebSocket (wss://)', { status: 426 });
    }

    // 2. Buat pasangan WebSocket (Client <-> Server)
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept();
    console.log('[Proxy] Browser connected');

    // 3. State Buffer
    let poolBuffer = '';
    let browserBuffer = '';
    let poolConnected = false;
    const queue = [];

    let writer = null;
    let reader = null;
    let tcpSocket = null;

    const encoder = new TextEncoder();
    const decoder = new TextDecoder();

    // Helper: Kirim data ke Browser
    function sendBrowser(line) {
      if (server.readyState !== 1 /* WebSocket.OPEN */) return;
      const payload = line.endsWith('\n') ? line : line + '\n';
      server.send(payload);
    }

    // Helper: Kirim data ke Stratum Pool
    async function sendPool(line) {
      if (!line) return;
      const payload = line.endsWith('\n') ? line : line + '\n';

      if (poolConnected && writer) {
        try {
          await writer.write(encoder.encode(payload));
        } catch (err) {
          console.log('[Proxy] TCP write error: ' + err.message);
        }
      } else {
        queue.push(payload);
        console.log(`[Proxy] Pool belum terhubung, message di-queue. Queue=${queue.length}`);
      }
    }

    // 4. Buka Koneksi TCP ke Upstream Stratum Pool (1miner.net:5102)
    console.log(`[Proxy] Connecting to ${STRATUM_HOST}:${STRATUM_PORT}...`);

    try {
      tcpSocket = connect({ hostname: STRATUM_HOST, port: STRATUM_PORT });
      writer = tcpSocket.writable.getWriter();
      reader = tcpSocket.readable.getReader();

      poolConnected = true;
      console.log(`[Proxy] Connected to ${STRATUM_HOST}:${STRATUM_PORT}`);

      // Flush queue pesan yang terpending
      while (queue.length > 0) {
        const payload = queue.shift();
        console.log(`[Proxy -> Pool queued] ${payload.trim()}`);
        await writer.write(encoder.encode(payload));
      }

      // Read Loop: Pool -> Proxy -> Browser
      (async () => {
        try {
          while (true) {
            const { value, done } = await reader.read();
            if (done) break;

            if (value) {
              poolBuffer += decoder.decode(value, { stream: true });
              let idx;

              while ((idx = poolBuffer.indexOf('\n')) !== -1) {
                const line = poolBuffer.slice(0, idx).trim();
                poolBuffer = poolBuffer.slice(idx + 1);

                if (!line) continue;

                // Logging Stratum Message
                try {
                  const msg = JSON.parse(line);
                  if (msg.method === 'mining.set_difficulty') {
                    console.log(`[Pool] difficulty=${msg.params?.[0]}`);
                  } else if (msg.method === 'mining.notify') {
                    console.log(`[Pool] job=${msg.params?.[0]} cleanJobs=${msg.params?.[8]}`);
                  } else if (msg.method === 'mining.set_extranonce') {
                    console.log(`[Pool] set_extranonce=${JSON.stringify(msg.params)}`);
                  } else if (msg.id !== undefined && (msg.result !== undefined || msg.error !== undefined)) {
                    console.log(`[Pool] response id=${msg.id} result=${JSON.stringify(msg.result)} error=${JSON.stringify(msg.error)}`);
                  }
                } catch (_) {
                  console.log('[Pool] Non-JSON line received');
                }

                console.log(`[Pool -> Browser] ${line}`);
                sendBrowser(line);
              }
            }
          }
        } catch (err) {
          console.error(`[Stratum Error] ${err.message}`);
          sendBrowser(JSON.stringify({
            jsonrpc: '2.0', id: null, result: null,
            error: [-1, `upstream stratum error: ${err.message}`, null]
          }));
        } finally {
          poolConnected = false;
          console.log('[Proxy] Upstream Stratum closed.');
          server.close(1011, 'Stratum connection closed');
        }
      })();

    } catch (err) {
      console.error(`[TCP Connection Error] ${err.message}`);
      return new Response('Gagal koneksi ke Stratum Pool: ' + err.message, { status: 500 });
    }

    // 5. Read Loop: Browser -> Proxy -> Pool
    server.addEventListener('message', async (event) => {
      browserBuffer += String(event.data);
      let idx;

      while ((idx = browserBuffer.indexOf('\n')) !== -1) {
        const line = browserBuffer.slice(0, idx).trim();
        browserBuffer = browserBuffer.slice(idx + 1);

        if (!line) continue;

        console.log(`[Browser -> Pool] ${line}`);
        await sendPool(line);
      }
    });

    // Handle Browser Disconnect
    server.addEventListener('close', () => {
      console.log('[Proxy] Browser disconnected');
      queue.length = 0;
      if (tcpSocket) {
        try { tcpSocket.close(); } catch (_) {}
      }
    });

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }
};
