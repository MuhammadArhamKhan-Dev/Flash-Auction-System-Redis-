import { useEffect, useRef, useState } from "react";
import { newBidId, useAuction } from "./useAuction.js";

const randomName = () => `bidder_${Math.floor(Math.random() * 900 + 100)}`;

function useRemaining(deadline) {
  const [now, setNow] = useState(performance.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(performance.now()), 100);
    return () => clearInterval(timer);
  }, []);
  return deadline ? Math.max(0, deadline - now) : 0;
}

export default function App() {
  const { connected, instance, auction, feed, send, log } = useAuction();
  const [name, setName] = useState(randomName);
  const [custom, setCustom] = useState("");
  const lastBid = useRef(null);
  const remaining = useRemaining(auction?.deadline);

  const open = connected && auction && remaining > 0;
  const seconds = remaining / 1000;

  function bid(amount) {
    if (!open || !name.trim() || !Number.isInteger(amount) || amount <= 0) return;
    lastBid.current = { bid_id: newBidId(), user_id: name.trim(), amount };
    send(lastBid.current);
  }

  function submitCustom(event) {
    event.preventDefault();
    bid(Number(custom));
    setCustom("");
  }

  async function reset(duration) {
    try {
      const response = await fetch(`/api/auction/1/reset?seconds=${duration}`, { method: "POST" });
      if (!response.ok) throw new Error();
      lastBid.current = null;
    } catch {
      log("Could not reset the auction", "bad");
    }
  }

  return (
    <main>
      <header>
        <h1>Flash Auction</h1>
        <span className={connected ? "badge good" : "badge bad"}>
          {connected ? `Live · ${instance}` : "Reconnecting…"}
        </span>
      </header>

      <section className="card center">
        <h2>{auction?.item ?? "Loading…"}</h2>
        <div className={`timer ${open && seconds <= 10 ? "urgent" : ""}`}>
          {!auction ? "--" : remaining > 0 ? `${seconds.toFixed(1)}s` : "Auction ended"}
        </div>
        <div className="price">${auction?.price ?? "--"}</div>
        <p>
          {auction?.leader
            ? `${remaining > 0 ? "Highest bidder" : "Winner"}: ${auction.leader}${auction.leader === name.trim() ? " (you)" : ""}`
            : "No bids yet"}
        </p>
      </section>

      <section className="card">
        <label>
          Your name
          <input value={name} maxLength={40} onChange={(e) => setName(e.target.value)} />
        </label>
        <div className="row">
          {[1, 10, 50].map((step) => (
            <button key={step} disabled={!open} onClick={() => bid(auction.price + step)}>
              Bid +${step}
            </button>
          ))}
        </div>
        <form className="row" onSubmit={submitCustom}>
          <input type="number" min="1" step="1" placeholder="Custom amount" value={custom}
                 onChange={(e) => setCustom(e.target.value)} />
          <button disabled={!open || !custom}>Place bid</button>
        </form>
        <button className="plain" disabled={!connected}
                onClick={() => (lastBid.current ? send(lastBid.current) : log("Place a bid first", "info"))}>
          Resend my last bid (idempotency test)
        </button>
      </section>

      <section className="card">
        <h3>Activity</h3>
        {feed.length === 0 && <p className="muted">Nothing yet.</p>}
        <ul>
          {feed.map((entry) => (
            <li key={entry.id} className={entry.kind}>
              <span className="muted">{entry.time}</span> {entry.text}
            </li>
          ))}
        </ul>
      </section>

      <section className="row">
        <span className="muted">Demo controls:</span>
        {[15, 60, 180].map((s) => (
          <button key={s} className="plain" onClick={() => reset(s)}>New {s}s round</button>
        ))}
      </section>
    </main>
  );
}
