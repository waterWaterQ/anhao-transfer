// ==================== 零依赖极简 MQTT 3.1.1 over WebSocket 信令客户端 ====================
// 仅用于交换 WebRTC 握手信息（SDP / ICE），文件与文字全程走点对点加密直连，绝不经过此通道

const SIGNAL_BROKERS = [
  "wss://broker-cn.emqx.io:8084/mqtt", // EMQX 中国大陆节点（国内不开 VPN 优先直连）
  "wss://broker.emqx.io:8084/mqtt",    // EMQX 备用公共节点
];

class MqttSignalClient {
  constructor() {
    this.ws = null;
    this.pingTimer = null;
    this.recvBuf = new Uint8Array(0);
    this.packetId = 1;
    this.closed = false;
    this.onMessage = null;
    this.onClose = null;
    this.encoder = new TextEncoder();
    this.decoder = new TextDecoder();
  }

  // 按顺序尝试国内节点，返回成功连上的节点地址
  async connect(timeoutPerBrokerMs = 6000) {
    this.closed = false;
    let lastErr = null;
    for (const url of SIGNAL_BROKERS) {
      if (this.closed) throw new Error("已取消连接");
      try {
        await this._connectSingle(url, timeoutPerBrokerMs);
        return url;
      } catch (err) {
        lastErr = err;
        this._cleanupSocket();
      }
    }
    throw lastErr || new Error("国内信令节点均无法连接");
  }

  _connectSingle(url, timeoutMs) {
    return new Promise((resolve, reject) => {
      let settled = false;
      const done = (ok, val) => {
        if (settled) return;
        settled = true;
        this.cancelConnect = null;
        clearTimeout(timer);
        ok ? resolve(val) : reject(val);
      };

      const timer = setTimeout(() => {
        done(false, new Error(`连接超时: ${url}`));
      }, timeoutMs);
      this.cancelConnect = () => done(false, new Error("已取消连接"));

      let ws;
      try {
        ws = new WebSocket(url, "mqtt");
      } catch (err) {
        done(false, err);
        return;
      }

      ws.binaryType = "arraybuffer";
      this.ws = ws;
      this.recvBuf = new Uint8Array(0);

      ws.onopen = () => {
        const clientId = "ah_" + Math.random().toString(36).slice(2, 12);
        ws.send(this._buildConnectPacket(clientId, 60));
      };

      ws.onmessage = (event) => {
        const chunk = new Uint8Array(event.data);
        this._appendAndParse(chunk, (packetType, payload) => {
          if (packetType === 2) {
            // CONNACK: payload[1] === 0 表示握手成功
            if (payload.length >= 2 && payload[1] === 0) {
              this._startPing();
              done(true, url);
            } else {
              done(false, new Error("信令服务器拒绝连接"));
            }
          } else if (packetType === 3) {
            // PUBLISH 消息到达
            this._handlePublishPayload(payload);
          }
        });
      };

      ws.onerror = () => {
        done(false, new Error(`连接异常: ${url}`));
      };

      ws.onclose = () => {
        this._stopPing();
        if (!settled) {
          done(false, new Error(`连接已关闭: ${url}`));
        } else if (!this.closed && this.onClose) {
          this.onClose();
        }
      };
    });
  }

  subscribe(topic) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    const pid = this.packetId++ & 0xffff || 1;
    const topicBytes = this.encoder.encode(topic);
    const bodyLen = 2 + 2 + topicBytes.length + 1;
    const remBytes = this._encodeRemainingLength(bodyLen);
    const packet = new Uint8Array(1 + remBytes.length + bodyLen);

    let o = 0;
    packet[o++] = 0x82; // SUBSCRIBE (QoS 1 固定标志位)
    packet.set(remBytes, o);
    o += remBytes.length;
    packet[o++] = (pid >> 8) & 0xff;
    packet[o++] = pid & 0xff;
    packet[o++] = (topicBytes.length >> 8) & 0xff;
    packet[o++] = topicBytes.length & 0xff;
    packet.set(topicBytes, o);
    o += topicBytes.length;
    packet[o++] = 0x00; // QoS 0

    this.ws.send(packet);
  }

  publish(topic, jsonObj) {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    const topicBytes = this.encoder.encode(topic);
    const payloadBytes = this.encoder.encode(JSON.stringify(jsonObj));
    const bodyLen = 2 + topicBytes.length + payloadBytes.length;
    const remBytes = this._encodeRemainingLength(bodyLen);
    const packet = new Uint8Array(1 + remBytes.length + bodyLen);

    let o = 0;
    packet[o++] = 0x30; // PUBLISH QoS 0
    packet.set(remBytes, o);
    o += remBytes.length;
    packet[o++] = (topicBytes.length >> 8) & 0xff;
    packet[o++] = topicBytes.length & 0xff;
    packet.set(topicBytes, o);
    o += topicBytes.length;
    packet.set(payloadBytes, o);

    this.ws.send(packet);
  }

  close() {
    this.closed = true;
    this.cancelConnect?.();
    this._stopPing();
    if (this.ws) {
      try {
        if (this.ws.readyState === WebSocket.OPEN) {
          this.ws.send(new Uint8Array([0xe0, 0x00])); // DISCONNECT
        }
        this.ws.close();
      } catch {}
    }
    this._cleanupSocket();
  }

  _cleanupSocket() {
    this._stopPing();
    if (this.ws) {
      this.ws.onopen = null;
      this.ws.onmessage = null;
      this.ws.onerror = null;
      this.ws.onclose = null;
      try { this.ws.close(); } catch {}
      this.ws = null;
    }
    this.recvBuf = new Uint8Array(0);
  }

  _startPing() {
    this._stopPing();
    this.pingTimer = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(new Uint8Array([0xc0, 0x00])); // PINGREQ
      }
    }, 25000);
  }

  _stopPing() {
    clearInterval(this.pingTimer);
    this.pingTimer = null;
  }

  _buildConnectPacket(clientId, keepAliveSec) {
    const idBytes = this.encoder.encode(clientId);
    const proto = [0x00, 0x04, 0x4d, 0x51, 0x54, 0x54, 0x04, 0x02, (keepAliveSec >> 8) & 0xff, keepAliveSec & 0xff];
    const bodyLen = proto.length + 2 + idBytes.length;
    const remBytes = this._encodeRemainingLength(bodyLen);
    const packet = new Uint8Array(1 + remBytes.length + bodyLen);

    let o = 0;
    packet[o++] = 0x10; // CONNECT
    packet.set(remBytes, o);
    o += remBytes.length;
    packet.set(proto, o);
    o += proto.length;
    packet[o++] = (idBytes.length >> 8) & 0xff;
    packet[o++] = idBytes.length & 0xff;
    packet.set(idBytes, o);

    return packet;
  }

  _encodeRemainingLength(len) {
    const out = [];
    let x = len;
    do {
      let byte = x % 128;
      x = Math.floor(x / 128);
      if (x > 0) byte |= 0x80;
      out.push(byte);
    } while (x > 0);
    return out;
  }

  // 解析可能分片或粘包的 MQTT 帧
  _appendAndParse(chunk, onPacket) {
    const merged = new Uint8Array(this.recvBuf.length + chunk.length);
    merged.set(this.recvBuf, 0);
    merged.set(chunk, this.recvBuf.length);
    this.recvBuf = merged;

    while (this.recvBuf.length >= 2) {
      let multiplier = 1;
      let remLen = 0;
      let idx = 1;
      let completeHeader = false;

      while (idx < this.recvBuf.length && idx <= 4) {
        const encodedByte = this.recvBuf[idx++];
        remLen += (encodedByte & 127) * multiplier;
        multiplier *= 128;
        if ((encodedByte & 128) === 0) {
          completeHeader = true;
          break;
        }
      }

      if (!completeHeader) break;
      const totalLen = idx + remLen;
      if (this.recvBuf.length < totalLen) break;

      const headerByte = this.recvBuf[0];
      const packetType = (headerByte >> 4) & 0x0f;
      const qos = (headerByte >> 1) & 0x03;
      const payload = this.recvBuf.slice(idx, totalLen);
      this.recvBuf = this.recvBuf.slice(totalLen);

      onPacket(packetType, payload, qos);
    }
  }

  _handlePublishPayload(payload) {
    if (payload.length < 2) return;
    const topicLen = (payload[0] << 8) | payload[1];
    const msgStart = 2 + topicLen;
    if (payload.length < msgStart) return;
    const topic = this.decoder.decode(payload.subarray(2, msgStart));
    const text = this.decoder.decode(payload.subarray(msgStart));
    try {
      const json = JSON.parse(text);
      if (this.onMessage) this.onMessage(topic, json);
    } catch {}
  }
}

window.MqttSignalClient = MqttSignalClient;
