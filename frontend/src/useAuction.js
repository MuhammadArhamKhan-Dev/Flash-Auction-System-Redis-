// WebSocket logic: live state, reconnecting, and safe retries.
import { useCallback, useEffect, useRef, useState } from "react";

const AUCTION_ID = 1;

// Open the page with ?port=8002 to connect straight to a particular backend (or ?port=8080 for Nginx).
function socketUrl() {
  const port = new URLSearchParams(window.location.search).get("port");
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const host = port ? `${window.location.hostname}:${port}` : window.location.host;
  return `${protocol}://${host}/ws/auction/${AUCTION_ID}`;
}

export function newBidId() {
  if (crypto.randomUUID) return crypto.randomUUID();
  // randomUUID is unavailable on plain http over a LAN address
  return "10000000-1000-4000-8000-100000000000".replace(/[018]/g, (c) =>
    (+c ^ (crypto.getRandomValues(new Uint8Array(1))[0] & (15 >> (+c / 4)))).toString(16)
  );
}

export function useAuction() {
  const [connected, setConnected] = useState(false);
  const [instance, setInstance] = useState("-");
  const [auction, setAuction] = useState(null);
  const [feed, setFeed] = useState([]);
  const socket = useRef(null);
  const latest = useRef(null);
  const pending = useRef(new Map()); // bids still waiting for an answer

  const log = useCallback((text, kind) => {
    setFeed((old) => [{ id: newBidId(), text, kind, time: new Date().toLocaleTimeString() }, ...old].slice(0, 30));
  }, []);

  useEffect(() => {
    let stopped = false;
    let retryTimer;
    let delay = 500;

    function connect() {
      const ws = new WebSocket(socketUrl());
      socket.current = ws;

      ws.onopen = () => {
        delay = 500;
        latest.current = null; // trust the fresh snapshot
        setConnected(true);
        // Resend unanswered bids with the SAME bid_id: the server never applies one twice.
        pending.current.forEach((bid) => ws.send(JSON.stringify(bid)));
      };

      ws.onmessage = (event) => {
        const msg = JSON.parse(event.data);
        if (msg.type === "hello") setInstance(msg.instance);

        if (msg.type === "state") {
          const old = latest.current;
          // Ignore anything older than what is already on screen.
          if (old && (msg.round < old.round || (msg.round === old.round && msg.version < old.version))) return;
          if (old && msg.round > old.round) log("A new auction round started", "info");
          else if (old && msg.version > old.version) log(`${msg.leader} leads at $${msg.price}`, "info");
          const next = { ...msg, deadline: performance.now() + msg.ends_in_ms };
          latest.current = next;
          setAuction(next);
        }

        if (msg.type === "bid_result") {
          pending.current.delete(msg.bid_id);
          if (msg.accepted) {
            log(`Your bid $${msg.amount} ${msg.duplicate ? "was already recorded (not applied twice)" : "was accepted"}`, "good");
          } else {
            log(`Your bid $${msg.amount} was rejected: ${msg.reason} (checked by ${msg.stage})`, "bad");
          }
        }

        if (msg.type === "error") log(`Server error: ${msg.reason}`, "bad");
      };

      ws.onclose = () => {
        setConnected(false);
        if (stopped) return;
        retryTimer = setTimeout(connect, delay + Math.random() * 300);
        delay = Math.min(delay * 2, 5000);
      };
    }

    connect();
    return () => {
      stopped = true;
      clearTimeout(retryTimer);
      socket.current?.close();
    };
  }, [log]);

  const send = useCallback((bid) => {
    if (socket.current?.readyState !== WebSocket.OPEN) return;
    pending.current.set(bid.bid_id, bid);
    socket.current.send(JSON.stringify(bid));
  }, []);

  return { connected, instance, auction, feed, send, log };
}
