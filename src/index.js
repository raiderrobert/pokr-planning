// ============================================================
// Durable Object: PlanningRoom
// Uses the WebSocket Hibernation API so rooms sleep when idle.
// ============================================================

export class PlanningRoom {
  constructor(state, env) {
    this.state = state;
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 400 });
    }

    const url = new URL(request.url);
    const pid = url.searchParams.get("pid") || crypto.randomUUID();

    // If this participant already has a connection, close the old one
    for (const old of this.state.getWebSockets(pid)) {
      old.close(1008, "Replaced by new connection");
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.state.acceptWebSocket(server, [pid]);
    server.serializeAttachment({ id: pid, name: null, vote: null, observer: false });

    return new Response(null, { status: 101, webSocket: client });
  }

  // --- Hibernation handlers -------------------------------------------

  async webSocketMessage(ws, message) {
    let data;
    try {
      data = JSON.parse(message);
    } catch {
      return;
    }

    const att = ws.deserializeAttachment();

    switch (data.type) {
      case "join": {
        att.name = data.name;
        ws.serializeAttachment(att);
        break;
      }

      case "vote": {
        att.vote = data.value;
        ws.serializeAttachment(att);
        break;
      }

      case "unvote": {
        att.vote = null;
        ws.serializeAttachment(att);
        break;
      }

      case "observer": {
        att.observer = !!data.value;
        if (att.observer) att.vote = null;
        ws.serializeAttachment(att);
        break;
      }

      case "reveal": {
        await this.state.storage.put("revealed", true);
        break;
      }

      case "reset": {
        await this.state.storage.put("revealed", false);
        for (const peer of this.state.getWebSockets()) {
          const a = peer.deserializeAttachment();
          a.vote = null;
          peer.serializeAttachment(a);
        }
        break;
      }

      case "topic": {
        await this.state.storage.put("topic", data.value ?? "");
        break;
      }

      default:
        return; // unknown message, don't broadcast
    }

    await this.#broadcast();
  }

  async webSocketClose(ws) {
    ws.close();
    await this.#broadcast();
  }

  async webSocketError(ws) {
    ws.close();
    await this.#broadcast();
  }

  // --- Helpers ---------------------------------------------------------

  async #broadcast() {
    const revealed = (await this.state.storage.get("revealed")) || false;
    const topic = (await this.state.storage.get("topic")) || "";

    const sockets = this.state.getWebSockets();
    const participants = [];

    for (const s of sockets) {
      const a = s.deserializeAttachment();
      if (!a.name) continue; // hasn't joined yet
      participants.push({
        id: a.id,
        name: a.name,
        voted: a.vote !== null,
        value: revealed ? a.vote : null,
        observer: !!a.observer,
      });
    }

    const msg = JSON.stringify({ type: "state", participants, revealed, topic });

    for (const s of this.state.getWebSockets()) {
      try {
        s.send(msg);
      } catch {
        /* disconnected */
      }
    }
  }
}

// ============================================================
// Worker — routes and serves static HTML
// ============================================================

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Landing page
    if (url.pathname === "/") {
      return html(landingHTML());
    }

    // Create room
    if (url.pathname === "/api/room" && request.method === "POST") {
      return Response.json({ id: generateRoomId() });
    }

    // Room page
    const roomMatch = url.pathname.match(/^\/room\/([a-zA-Z0-9]+)$/);
    if (roomMatch) {
      return html(roomHTML(roomMatch[1]));
    }

    // WebSocket upgrade → forward to Durable Object
    const wsMatch = url.pathname.match(/^\/api\/room\/([a-zA-Z0-9]+)\/ws$/);
    if (wsMatch) {
      const id = env.PLANNING_ROOM.idFromName(wsMatch[1]);
      const stub = env.PLANNING_ROOM.get(id);
      return stub.fetch(request);
    }

    return new Response("Not found", { status: 404 });
  },
};

function html(body) {
  return new Response(body, {
    headers: { "Content-Type": "text/html;charset=UTF-8" },
  });
}

function generateRoomId() {
  const chars = "abcdefghijkmnpqrstuvwxyz23456789";
  const bytes = new Uint8Array(6);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => chars[b % chars.length]).join("");
}

// ============================================================
// HTML — Landing Page
// ============================================================

function landingHTML() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pokr Planning</title>
<style>
${SHARED_STYLES}
body { display:flex; align-items:center; justify-content:center; min-height:100dvh; }
.landing { text-align:center; max-width:400px; padding:2rem; }
.landing h1 { font-size:2.5rem; margin:0 0 .25rem; }
.landing h1 span { color:var(--primary); }
.landing p.sub { color:var(--text2); margin:0 0 2rem; font-size:1.05rem; }
.landing input {
  width:100%; padding:.75rem 1rem; border:2px solid var(--border);
  border-radius:var(--r); font-size:1rem; margin-bottom:1rem;
  background:var(--surface); color:var(--text);
  transition:border-color .15s;
}
.landing input:focus { outline:none; border-color:var(--primary); }
.landing button {
  width:100%; padding:.85rem; border:none; border-radius:var(--r);
  background:var(--primary); color:#fff; font-size:1.05rem;
  font-weight:600; cursor:pointer; transition:background .15s;
}
.landing button:hover { background:var(--primary-h); }
.landing button:disabled { opacity:.5; cursor:default; }
.divider { color:var(--text2); margin:1.25rem 0; font-size:.85rem; }
.join-row { display:flex; gap:.5rem; }
.join-row input { margin:0; flex:1; }
.join-row button { width:auto; padding:.75rem 1.25rem; background:var(--text2); }
.join-row button:hover { background:var(--text); }
</style>
</head>
<body>
<div class="landing">
  <h1>🃏 Pokr <span>Planning</span></h1>
  <p class="sub">Estimate together, ship faster</p>

  <input id="nameInput" type="text" placeholder="Your name" autocomplete="off" maxlength="30">
  <button id="createBtn" onclick="createRoom()">Create Room</button>

  <div class="divider">— or join an existing room —</div>

  <div class="join-row">
    <input id="codeInput" type="text" placeholder="Room code" maxlength="10">
    <button onclick="joinRoom()">Join</button>
  </div>
</div>
<script>
  const nameInput = document.getElementById('nameInput');
  const codeInput = document.getElementById('codeInput');
  nameInput.value = localStorage.getItem('pokr-name') || '';

  async function createRoom() {
    saveName();
    if (!nameInput.value.trim()) { nameInput.focus(); return; }
    const res = await fetch('/api/room', { method: 'POST' });
    const { id } = await res.json();
    location.href = '/room/' + id;
  }

  function joinRoom() {
    saveName();
    const code = codeInput.value.trim().toLowerCase();
    if (!code) { codeInput.focus(); return; }
    if (!nameInput.value.trim()) { nameInput.focus(); return; }
    location.href = '/room/' + code;
  }

  function saveName() {
    localStorage.setItem('pokr-name', nameInput.value.trim());
  }

  // Enter key handling
  nameInput.addEventListener('keydown', e => { if (e.key === 'Enter') createRoom(); });
  codeInput.addEventListener('keydown', e => { if (e.key === 'Enter') joinRoom(); });
</script>
</body>
</html>`;
}

// ============================================================
// HTML — Room Page
// ============================================================

function roomHTML(roomId) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pokr Planning — Room ${roomId}</title>
<style>
${SHARED_STYLES}

/* --- Name Modal --- */
.modal-overlay {
  position:fixed; inset:0; background:rgba(0,0,0,.45); z-index:100;
  display:flex; align-items:center; justify-content:center;
  backdrop-filter:blur(4px);
}
.modal {
  background:var(--surface); border-radius:var(--r2); padding:2rem;
  width:min(360px,90vw); text-align:center;
  box-shadow:0 20px 60px rgba(0,0,0,.2);
}
.modal h2 { margin:0 0 .25rem; font-size:1.3rem; }
.modal p { margin:0 0 1.25rem; color:var(--text2); font-size:.9rem; }
.modal input {
  width:100%; padding:.7rem 1rem; border:2px solid var(--border);
  border-radius:var(--r); font-size:1rem; margin-bottom:1rem;
  background:var(--bg); color:var(--text);
}
.modal input:focus { outline:none; border-color:var(--primary); }
.modal button {
  width:100%; padding:.75rem; border:none; border-radius:var(--r);
  background:var(--primary); color:#fff; font-size:1rem;
  font-weight:600; cursor:pointer;
}
.modal button:hover { background:var(--primary-h); }

/* --- Header --- */
header {
  display:flex; align-items:center; justify-content:space-between;
  padding:.75rem 1.25rem; background:var(--surface);
  border-bottom:1px solid var(--border);
}
header .logo { font-weight:700; font-size:1.15rem; }
header .logo span { color:var(--primary); }
.room-info { display:flex; align-items:center; gap:.5rem; }
.new-room-btn {
  background:none; border:1px solid var(--border); border-radius:6px;
  padding:.3rem .6rem; cursor:pointer; font-size:.8rem; color:var(--text2);
  text-decoration:none; font-weight:500; transition:all .15s;
}
.new-room-btn:hover { border-color:var(--primary); color:var(--primary); }
.observer-toggle {
  background:none; border:1px solid var(--border); border-radius:6px;
  padding:.3rem .6rem; cursor:pointer; font-size:.8rem; color:var(--text2);
  font-weight:500; transition:all .15s;
}
.observer-toggle:hover { border-color:var(--primary); color:var(--primary); }
.observer-toggle.active { background:var(--primary); color:#fff; border-color:var(--primary); }
.room-code {
  background:var(--bg); padding:.3rem .65rem; border-radius:6px;
  font-family:"SF Mono",ui-monospace,monospace; font-size:.85rem;
  color:var(--text2);
}
.copy-btn {
  background:none; border:1px solid var(--border); border-radius:6px;
  padding:.3rem .6rem; cursor:pointer; font-size:.8rem; color:var(--text2);
  transition:all .15s;
}
.copy-btn:hover { border-color:var(--primary); color:var(--primary); }
.copy-btn.copied { border-color:var(--green); color:var(--green); }

/* --- Topic --- */
.topic-bar {
  padding:.75rem 1.25rem; background:var(--surface);
  border-bottom:1px solid var(--border);
}
.topic-bar input {
  width:100%; padding:.5rem .75rem; border:1px dashed var(--border);
  border-radius:var(--r); font-size:.95rem; background:transparent;
  color:var(--text); transition:border-color .15s;
}
.topic-bar input:focus { outline:none; border-style:solid; border-color:var(--primary); }

/* --- Main Layout --- */
main { max-width:900px; margin:0 auto; padding:1.5rem 1.25rem 8rem; }

/* --- Participants --- */
.participants-label {
  font-size:.8rem; text-transform:uppercase; letter-spacing:.06em;
  color:var(--text2); font-weight:600; margin-bottom:.75rem;
}
.participants {
  display:grid;
  grid-template-columns:repeat(auto-fill,minmax(140px,1fr));
  gap:.75rem;
}
.p-card {
  background:var(--surface); border-radius:var(--r);
  padding:1rem; text-align:center;
  box-shadow:var(--shadow);
  transition:transform .2s;
}

/* Card flip container */
.p-card .flip-card {
  width:56px; height:72px; margin:0 auto .6rem;
  perspective:500px;
}
.p-card .flip-inner {
  width:100%; height:100%; position:relative;
  transition:transform .5s;
  transform-style:preserve-3d;
}
.p-card.revealed .flip-inner { transform:rotateY(180deg); }
.p-card .flip-front, .p-card .flip-back {
  position:absolute; inset:0; border-radius:8px;
  display:flex; align-items:center; justify-content:center;
  backface-visibility:hidden; font-weight:700;
}
.p-card .flip-front {
  background:var(--border); color:var(--text2); font-size:1.1rem;
}
.p-card .flip-front.voted {
  background:var(--primary); color:#fff;
}
.p-card .flip-back {
  background:var(--primary); color:#fff; font-size:1.4rem;
  transform:rotateY(180deg);
}
.p-card .p-name {
  font-size:.85rem; font-weight:500; color:var(--text);
  white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
}
.p-card.observer { opacity:.6; }
.p-card.observer .flip-front { background:transparent; border:2px dashed var(--border); color:var(--text2); }
.p-card .p-role {
  font-size:.7rem; color:var(--text2); margin-top:.15rem;
}

/* --- Stats Bar --- */
.stats {
  margin-top:1.25rem; display:flex; gap:.75rem; flex-wrap:wrap;
}
.stat {
  background:var(--surface); border-radius:var(--r); padding:.6rem 1rem;
  box-shadow:var(--shadow); text-align:center; min-width:80px;
}
.stat .label { font-size:.7rem; text-transform:uppercase; color:var(--text2);
  letter-spacing:.04em; font-weight:600; }
.stat .val { font-size:1.3rem; font-weight:700; color:var(--primary); margin-top:.15rem; }

/* --- Action Buttons --- */
.actions {
  display:flex; gap:.5rem; margin-top:1.25rem; flex-wrap:wrap;
}
.actions button {
  padding:.6rem 1.4rem; border:none; border-radius:var(--r);
  font-size:.9rem; font-weight:600; cursor:pointer; transition:all .15s;
}
.btn-reveal {
  background:var(--primary); color:#fff;
}
.btn-reveal:hover { background:var(--primary-h); }
.btn-reveal.ready { animation:pulse 1.5s infinite; }
@keyframes pulse {
  0%,100% { box-shadow:0 0 0 0 rgba(99,102,241,.4); }
  50% { box-shadow:0 0 0 8px rgba(99,102,241,0); }
}
.btn-reset {
  background:var(--primary); color:#fff; border:2px solid var(--primary);
  font-size:1rem; padding:.75rem 2rem;
}
.btn-reset:hover { background:var(--primary-h); border-color:var(--primary-h); }

/* --- Voting Dock --- */
.dock {
  position:fixed; bottom:0; left:0; right:0;
  background:var(--surface); border-top:1px solid var(--border);
  padding:.75rem 1rem; z-index:50;
  box-shadow:0 -4px 20px rgba(0,0,0,.06);
}
.dock-inner {
  max-width:700px; margin:0 auto;
  display:flex; gap:.4rem; justify-content:center; flex-wrap:wrap;
}
.vote-card {
  width:48px; height:64px; border-radius:8px;
  border:2px solid var(--border); background:var(--surface);
  display:flex; align-items:center; justify-content:center;
  font-size:1rem; font-weight:700; cursor:pointer;
  color:var(--text); transition:all .12s;
  user-select:none;
}
.vote-card:hover {
  border-color:var(--primary); color:var(--primary);
  transform:translateY(-4px);
}
.vote-card.selected {
  background:var(--primary); color:#fff; border-color:var(--primary);
  transform:translateY(-6px);
  box-shadow:0 4px 12px rgba(99,102,241,.35);
}
.vote-card.disabled {
  opacity:.4; pointer-events:none;
}

/* --- Connection status --- */
.conn-status {
  position:fixed; top:.5rem; right:.5rem; font-size:.7rem;
  padding:.25rem .5rem; border-radius:20px; z-index:200;
  font-weight:600;
}
.conn-status.ok { background:#d1fae5; color:#065f46; }
.conn-status.err { background:#fee2e2; color:#991b1b; }

/* --- Stagger flip delay --- */
${Array.from({ length: 20 }, (_, i) =>
  `.p-card:nth-child(${i + 1}) .flip-inner { transition-delay:${i * 0.07}s; }`
).join("\n")}
</style>
</head>
<body>

<!-- Name modal (shown if no name in localStorage) -->
<div class="modal-overlay" id="nameModal" style="display:none">
  <div class="modal">
    <h2>Join the room</h2>
    <p>Pick a name so your team knows who you are.</p>
    <input id="modalName" type="text" placeholder="Your name" maxlength="30" autocomplete="off">
    <button onclick="submitName()">Join</button>
  </div>
</div>

<div class="conn-status" id="connStatus"></div>

<header>
  <div class="logo">🃏 Pokr <span>Planning</span></div>
  <div class="room-info">
    <span class="room-code" id="roomCode">${roomId}</span>
    <button class="copy-btn" onclick="copyLink(this)">Copy link</button>
    <a class="new-room-btn" href="/">+ New Room</a>
    <button class="observer-toggle" id="observerToggle" onclick="toggleObserver()">👁 Observer</button>
  </div>
</header>

<div class="topic-bar">
  <input id="topicInput" type="text" placeholder="What are we estimating? (e.g. JIRA-123: Add login flow)" autocomplete="off">
</div>

<main>
  <div class="participants-label" id="pLabel">PARTICIPANTS</div>
  <div class="participants" id="participants"></div>
  <div class="stats" id="stats"></div>
  <div class="actions" id="actions"></div>
</main>

<div class="dock" id="dock">
  <div class="dock-inner" id="voteDock"></div>
</div>

<script>
// ---- Config ----
const ROOM_ID = "${roomId}";
const CARD_VALUES = ["0","½","1","2","3","5","8","13","21","?","☕"];

// ---- State ----
let ws = null;
let myPid = localStorage.getItem("pokr-pid");
if (!myPid) { myPid = crypto.randomUUID(); localStorage.setItem("pokr-pid", myPid); }
let myName = localStorage.getItem("pokr-name") || "";
let selectedVote = null;
let isObserver = localStorage.getItem("pokr-observer") === "true";
let roomState = null;
let reconnectDelay = 500;
let topicTimer = null;

// ---- Name modal ----
if (!myName) document.getElementById("nameModal").style.display = "flex";
document.getElementById("modalName").addEventListener("keydown", e => { if (e.key === "Enter") submitName(); });

function submitName() {
  const v = document.getElementById("modalName").value.trim();
  if (!v) return;
  myName = v;
  localStorage.setItem("pokr-name", myName);
  document.getElementById("nameModal").style.display = "none";
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: "join", name: myName }));
  } else {
    connect();
  }
}

// ---- WebSocket ----
function connect() {
  if (!myName) return;
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  ws = new WebSocket(proto + "//" + location.host + "/api/room/" + ROOM_ID + "/ws?pid=" + myPid);

  ws.onopen = () => {
    reconnectDelay = 500;
    showConn(true);
    ws.send(JSON.stringify({ type: "join", name: myName }));
    ws.send(JSON.stringify({ type: "observer", value: isObserver }));
    if (!isObserver && selectedVote !== null) {
      ws.send(JSON.stringify({ type: "vote", value: selectedVote }));
    }
  };

  ws.onmessage = (e) => {
    const data = JSON.parse(e.data);
    if (data.type === "state") {
      roomState = data;
      render();
    }
  };

  ws.onclose = () => {
    showConn(false);
    setTimeout(connect, Math.min(reconnectDelay, 8000));
    reconnectDelay *= 1.5;
  };

  ws.onerror = () => ws.close();
}

function showConn(ok) {
  const el = document.getElementById("connStatus");
  el.textContent = ok ? "Connected" : "Reconnecting…";
  el.className = "conn-status " + (ok ? "ok" : "err");
  if (ok) setTimeout(() => { el.style.opacity = "0"; }, 2000);
  else el.style.opacity = "1";
}

// ---- Observer toggle ----
function toggleObserver() {
  isObserver = !isObserver;
  localStorage.setItem("pokr-observer", isObserver);
  if (isObserver) { selectedVote = null; }
  ws.send(JSON.stringify({ type: "observer", value: isObserver }));
  updateObserverBtn();
}

function updateObserverBtn() {
  const btn = document.getElementById("observerToggle");
  btn.classList.toggle("active", isObserver);
  btn.textContent = isObserver ? "👁 Observer (on)" : "👁 Observer";
}

// ---- Voting ----
function vote(value) {
  if (isObserver) return;
  if (roomState && roomState.revealed) return;
  if (selectedVote === value) {
    // Deselect
    selectedVote = null;
    ws.send(JSON.stringify({ type: "unvote" }));
  } else {
    selectedVote = value;
    ws.send(JSON.stringify({ type: "vote", value }));
  }
  renderDock();
}

function reveal() {
  ws.send(JSON.stringify({ type: "reveal" }));
}

function reset() {
  selectedVote = null;
  ws.send(JSON.stringify({ type: "reset" }));
}

// ---- Topic ----
document.getElementById("topicInput").addEventListener("input", (e) => {
  clearTimeout(topicTimer);
  topicTimer = setTimeout(() => {
    ws.send(JSON.stringify({ type: "topic", value: e.target.value }));
  }, 300);
});

// ---- Copy link ----
function copyLink(btn) {
  navigator.clipboard.writeText(location.href).then(() => {
    btn.textContent = "Copied!";
    btn.classList.add("copied");
    setTimeout(() => { btn.textContent = "Copy link"; btn.classList.remove("copied"); }, 2000);
  });
}

// ---- Render ----
function render() {
  if (!roomState) return;

  // Topic (only update if not focused, to avoid overwriting while typing)
  const topicInput = document.getElementById("topicInput");
  if (document.activeElement !== topicInput) {
    topicInput.value = roomState.topic;
  }

  // Participants
  const container = document.getElementById("participants");
  const people = roomState.participants;

  container.innerHTML = people.map(p => {
    const isMe = p.id === myPid;
    const revealedClass = roomState.revealed && !p.observer ? "revealed" : "";
    const votedClass = p.voted ? "voted" : "";
    const observerClass = p.observer ? "observer" : "";
    const displayValue = roomState.revealed && p.voted ? p.value : "";
    const frontSymbol = p.observer ? "👁" : (p.voted ? "✓" : "·");
    return \`
      <div class="p-card \${revealedClass} \${observerClass}">
        <div class="flip-card">
          <div class="flip-inner">
            <div class="flip-front \${votedClass}">\${frontSymbol}</div>
            <div class="flip-back">\${displayValue}</div>
          </div>
        </div>
        <div class="p-name">\${esc(p.name)}\${isMe ? " (you)" : ""}</div>
        \${p.observer ? '<div class="p-role">observer</div>' : ''}
      </div>
    \`;
  }).join("");

  // Label
  const voters = people.filter(p => !p.observer);
  const voteCount = voters.filter(p => p.voted).length;
  const observerCount = people.filter(p => p.observer).length;
  const obsLabel = observerCount > 0 ? \` · \${observerCount} observing\` : "";
  document.getElementById("pLabel").textContent =
    \`PARTICIPANTS — \${voteCount}/\${voters.length} voted\${obsLabel}\`;

  // Stats (only when revealed)
  const statsEl = document.getElementById("stats");
  if (roomState.revealed && people.some(p => p.voted)) {
    const numericVotes = people
      .filter(p => p.voted && p.value !== "?" && p.value !== "☕")
      .map(p => p.value === "½" ? 0.5 : Number(p.value))
      .filter(v => !isNaN(v))
      .sort((a, b) => a - b);

    let html = "";
    if (numericVotes.length > 0) {
      const avg = numericVotes.reduce((a, b) => a + b, 0) / numericVotes.length;
      const mid = numericVotes.length % 2 === 0
        ? (numericVotes[numericVotes.length / 2 - 1] + numericVotes[numericVotes.length / 2]) / 2
        : numericVotes[Math.floor(numericVotes.length / 2)];
      const lo = numericVotes[0];
      const hi = numericVotes[numericVotes.length - 1];
      const consensus = lo === hi;

      html += \`<div class="stat"><div class="label">Average</div><div class="val">\${avg.toFixed(1)}</div></div>\`;
      html += \`<div class="stat"><div class="label">Median</div><div class="val">\${mid}</div></div>\`;
      html += \`<div class="stat"><div class="label">Spread</div><div class="val">\${lo} – \${hi}</div></div>\`;
      if (consensus) {
        html += \`<div class="stat"><div class="label">Consensus</div><div class="val">✅</div></div>\`;
      }
    }
    statsEl.innerHTML = html;
  } else {
    statsEl.innerHTML = "";
  }

  // Action buttons
  const actionsEl = document.getElementById("actions");
  if (roomState.revealed) {
    actionsEl.innerHTML = '<button class="btn-reset" onclick="reset()">🔄 New Round</button>';
  } else {
    const allVoted = voters.length > 0 && voters.every(p => p.voted);
    actionsEl.innerHTML = \`
      <button class="btn-reveal \${allVoted ? 'ready' : ''}" onclick="reveal()">
        Reveal Votes
      </button>
    \`;
  }

  renderDock();
}

function renderDock() {
  const disabled = isObserver || (roomState && roomState.revealed);
  const dock = document.getElementById("voteDock");
  dock.innerHTML = CARD_VALUES.map(v => {
    const sel = selectedVote === v ? "selected" : "";
    const dis = disabled ? "disabled" : "";
    return \`<div class="vote-card \${sel} \${dis}" onclick="vote('\${v}')">\${v}</div>\`;
  }).join("");
}

function esc(s) {
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}

// ---- Boot ----
updateObserverBtn();
if (myName) connect();
renderDock();
</script>
</body>
</html>`;
}

// ============================================================
// Shared CSS
// ============================================================

const SHARED_STYLES = `
*,*::before,*::after { box-sizing:border-box; }
:root {
  --bg:#f5f5f7; --surface:#ffffff; --primary:#6366f1;
  --primary-h:#4f46e5; --text:#1e1b4b; --text2:#6b7280;
  --border:#e5e7eb; --green:#10b981;
  --shadow:0 1px 3px rgba(0,0,0,.08);
  --r:10px; --r2:16px;
}
body {
  margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,
    Helvetica,Arial,sans-serif; background:var(--bg); color:var(--text);
}
`;
