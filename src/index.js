import { connect } from "cloudflare:sockets";
/**
 * V0.1
 *
 * Telegram DC2:
 *   WSS -> Cloudflare Worker -> TCP:443 -> Telegram DC2
 *
 * 目前只允许 Telegram DC IP。
 * 不允许用户把 Worker 当任意 TCP 代理使用。
 */
const DC_TARGETS = {
  "2": [
    "149.154.167.50",
    "149.154.167.41",
    "149.154.167.220",
  ],
};
const PATH = "/apiws";
function getTarget(dc) {
  const targets = DC_TARGETS[dc];
  if (!targets || targets.length === 0) {
    return null;
  }
  // V0.1 固定第一个，后面再做故障切换。
  return targets[0];
}
function isWebSocket(request) {
  return (
    (request.headers.get("Upgrade") || "").toLowerCase() === "websocket"
  );
}
function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}
export default {
  async fetch(request) {
    const url = new URL(request.url);
    /*
     * 健康检查
     */
    if (url.pathname === "/") {
      return json({
        name: "CF Telegram Worker",
        version: "0.1.0",
        status: "ok",
        mode: "telegram-dc2-tcp-tunnel",
        websocket: "/apiws?dc=2",
      });
    }
    /*
     * 只允许 WebSocket
     */
    if (url.pathname !== PATH) {
      return new Response("Not Found", {
        status: 404,
        headers: {
          "content-type": "text/plain; charset=utf-8",
        },
      });
    }
    if (!isWebSocket(request)) {
      return new Response("Expected WebSocket", {
        status: 426,
        headers: {
          "Upgrade": "websocket",
        },
      });
    }
    /*
     * V0.1 只支持 DC2
     */
    const dc = url.searchParams.get("dc") || "2";
    const target = getTarget(dc);
    if (!target) {
      return json(
        {
          error: "unsupported_dc",
          message: "V0.1 only supports Telegram DC2.",
        },
        400,
      );
    }
    /*
     * 创建 WebSocket
     */
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept({
      allowHalfOpen: true,
    });
    let socket;
    let tcpReader;
    let tcpWriter;
    try {
      /*
       * Cloudflare Worker -> Telegram DC
       */
      socket = connect({
        hostname: target,
        port: 443,
      });
      await socket.opened;
      tcpReader = socket.readable.getReader();
      tcpWriter = socket.writable.getWriter();
      /*
       * WebSocket -> TCP
       */
      server.addEventListener("message", async (event) => {
        try {
          let data;
          if (event.data instanceof ArrayBuffer) {
            data = new Uint8Array(event.data);
          } else if (event.data instanceof Blob) {
            data = new Uint8Array(await event.data.arrayBuffer());
          } else if (typeof event.data === "string") {
            /*
             * Telegram MTProto transport 应该使用 binary。
             * V0.1 不接受字符串数据。
             */
            server.close(1003, "Binary WebSocket frames required");
            return;
          } else {
            server.close(1003, "Unsupported WebSocket data");
            return;
          }
          if (data.byteLength > 0) {
            await tcpWriter.write(data);
          }
        } catch (error) {
          console.error("WebSocket -> TCP failed:", error);
          try {
            server.close(1011, "TCP write failed");
          } catch {}
        }
      });
      /*
       * WebSocket 关闭 -> TCP 关闭
       */
      server.addEventListener("close", async () => {
        try {
          await tcpWriter.close();
        } catch {}
        try {
          await socket.close();
        } catch {}
      });
      /*
       * TCP -> WebSocket
       */
      (async () => {
        try {
          while (true) {
            const { value, done } = await tcpReader.read();
            if (done) {
              break;
            }
            if (!value) {
              continue;
            }
            if (server.readyState === WebSocket.OPEN) {
              server.send(value);
            } else {
              break;
            }
          }
        } catch (error) {
          console.error("TCP -> WebSocket failed:", error);
        } finally {
          try {
            tcpReader.releaseLock();
          } catch {}
          try {
            await socket.close();
          } catch {}
          try {
            if (server.readyState !== WebSocket.CLOSED) {
              server.close(1000, "TCP connection closed");
            }
          } catch {}
        }
      })();
      /*
       * 101 Switching Protocols
       */
      return new Response(null, {
        status: 101,
        webSocket: client,
      });
    } catch (error) {
      console.error("TCP connection failed:", error);
      try {
        server.close(1011, "Telegram DC connection failed");
      } catch {}
      try {
        await socket?.close();
      } catch {}
      return new Response("Unable to connect to Telegram DC", {
        status: 502,
      });
    }
  },
};
