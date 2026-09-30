/* Desk command center. One-answer chat stays on the existing room socket. */
(function () {
  const ROUTES = ["home", "desk", "tasks", "briefings", "more", "apps"];
  const TASK_VIEWS = [
    ["today", "Today"],
    ["upcoming", "Upcoming"],
    ["waiting", "Waiting"],
    ["completed", "Completed"],
  ];
  const BRIEF_TABS = [
    ["for_you", "For you"],
    ["markets", "Markets"],
    ["watchlist", "Watchlist"],
    ["life", "Life"],
  ];
  const NAMES = { grok: "Rail", gemini: "Anchor", ace: "Ace", shavor: "Shavor" };

  const state = {
    route: "home",
    token: "",
    signedOut: false,
    connected: false,
    dashState: "loading",
    dashError: "",
    dashboard: null,
    messages: [],
    mode: "one_answer",
    crosstalk: false,
    draft: "",
    search: "",
    searchOpen: false,
    contextOpen: false,
    menu: null,
    thinking: {},
    requestState: "",
    speakOn: false,
    versionLabel: "",
    taskView: "today",
    tasks: [],
    briefings: [],
    gainers: null,
    gainerSource: "combined",
    integrations: null,
    undo: null,
    briefTab: "for_you",
    routeArg: "",
    offline: !navigator.onLine,
  };

  try {
    state.mode = localStorage.getItem("desk_mode") || "one_answer";
    state.crosstalk = localStorage.getItem("desk_xtalk") === "1";
    state.speakOn = localStorage.getItem("desk_speak") === "1";
  } catch (e) { /* private mode */ }

  const app = document.getElementById("app");
  let ws = null;
  let socketGen = 0;
  let pressTimer = null;

  function esc(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function greeting() {
    const hour = new Date().getHours();
    if (hour < 12) return "Good morning";
    if (hour < 17) return "Good afternoon";
    return "Good evening";
  }

  function fmtTs(ts) {
    if (!ts) return "";
    const date = new Date(ts);
    if (Number.isNaN(date.getTime())) return "";
    return date.toLocaleString(undefined, {
      month: "short", day: "numeric", hour: "numeric", minute: "2-digit",
    });
  }

  function spade() {
    return '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M12 2.2c2.2 3.4 6.8 6.6 6.8 10.4 0 2.5-1.9 4.2-4.2 4.2-1.3 0-2.3-.6-3-1.5.2 1.6.2 2.7.1 3.5H16v2.2H8v-2.2h4.2c.1-.9 0-2.1-.3-3.6-.7.9-1.8 1.6-3.1 1.6-2.3 0-4.2-1.8-4.2-4.3C4.6 8.7 9.4 5.4 12 2.2z"/></svg>';
  }

  function icon(name) {
    const paths = {
      home: "M4 10.5 12 4l8 6.5V20a1 1 0 0 1-1 1h-5v-6H10v6H5a1 1 0 0 1-1-1z",
      desk: "M5 6h14a1 1 0 0 1 1 1v8.5a1 1 0 0 1-1 1H13l-3.2 2.4c-.5.4-1.3 0-1.3-.7V16.5H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1z",
      tasks: "M6 4h12v16H6zM8.5 8.5l1.4 1.4 2.6-2.8M8.5 13.2l1.4 1.4 2.6-2.8",
      brief: "M6 3.5h9l3 3V20.5H6zM15 3.8V8h4",
      more: "M6 12h.01M12 12h.01M18 12h.01",
    };
    return '<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" d="' + paths[name] + '"/></svg>';
  }

  function routeFromHash() {
    const raw = (location.hash || "#/home").replace(/^#\/?/, "").split("?")[0];
    const bits = raw.split("/").filter(Boolean);
    const name = bits[0] || "home";
    state.routeArg = bits[1] || "";
    if (name === "apps") return "apps";
    return ROUTES.indexOf(name) >= 0 ? name : "home";
  }

  function go(route, arg) {
    if (ROUTES.indexOf(route) < 0) route = "home";
    state.routeArg = arg || "";
    const next = "#/" + route + (state.routeArg ? "/" + state.routeArg : "");
    if (location.hash !== next) history.pushState({ route: route }, "", next);
    state.route = route;
    state.menu = null;
    render();
    loadRouteData();
  }

  function loadRouteData() {
    if (!state.token && !state.authed) return;
    if (state.route === "home" || state.route === "more") loadDashboard();
    if (state.route === "tasks") loadTasks();
    if (state.route === "briefings") { loadBriefings(); loadGainers(); }
    if (state.route === "apps") loadIntegrations();
  }

  function uuid() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    return "m-" + Date.now().toString(16) + "-" + Math.random().toString(16).slice(2);
  }

  function api(path, options) {
    const opts = options || {};
    opts.cache = "no-store";
    opts.credentials = "include";
    let url = path;
    if (state.token) {
      url += (path.indexOf("?") >= 0 ? "&" : "?") + "token=" + encodeURIComponent(state.token);
    }
    return fetch(url, opts);
  }

  async function loadDashboard() {
    if (!state.token && !state.authed) return;
    state.dashState = state.offline ? "offline" : "loading";
    try {
      const response = await api("/api/v2/dashboard");
      if (!response.ok) throw new Error("Home didn't load (" + response.status + ").");
      state.dashboard = await response.json();
      state.dashState = "ready";
      state.dashError = "";
    } catch (error) {
      state.dashState = navigator.onLine ? "error" : "offline";
      state.dashError = error.message || "Couldn't reach the desk.";
    }
    render();
  }

  function connectionPill() {
    if (state.offline) return '<span class="pill bad"><i class="dot"></i>Offline</span>';
    if (state.connected) return '<span class="pill live"><i class="dot"></i>Live</span>';
    return '<span class="pill wait"><i class="dot"></i>Connecting</span>';
  }

  function nav() {
    const items = [
      ["home", "Home", "home"],
      ["desk", "Desk", "desk"],
      ["tasks", "Tasks", "tasks"],
      ["briefings", "Briefings", "brief"],
      ["more", "More", "more"],
    ];
    return '<nav class="tabbar" aria-label="Primary">' + items.map(function (item) {
      const current = state.route === item[0] ? ' aria-current="page"' : "";
      return '<button class="tab" type="button" data-action="go" data-route="' + item[0] + '"' + current + '>' +
        icon(item[2]) + "<span>" + item[1] + "</span></button>";
    }).join("") + "</nav>";
  }

  function shell(body) {
    return '<div class="shell">' + body + "</div>" + snack() + nav() + sheet();
  }

  function snack() {
    if (!state.undo) return "";
    return '<div class="snack" role="status"><span>Task completed</span><button type="button" data-action="undo-task">Undo</button></div>';
  }

  function sheet() {
    if (!state.menu) return "";
    const buttons = state.menu.actions.map(function (action) {
      return '<button type="button" data-action="' + esc(action[0]) + '"' +
        (action[2] ? ' data-arg="' + esc(action[2]) + '"' : "") + ">" + esc(action[1]) + "</button>";
    }).join("");
    return '<div class="sheet-back" data-action="close-menu"><div class="sheet" role="dialog" aria-label="' +
      esc(state.menu.label) + '">' + buttons + "</div></div>";
  }

  function homeScreen() {
    if (state.dashState === "loading" && !state.dashboard) {
      return screenHead(greeting() + ", Shavor", connectionPill()) +
        '<p class="banner">Loading the desk…</p>';
    }
    if (state.dashState === "offline" && !state.dashboard) {
      return screenHead(greeting() + ", Shavor", connectionPill()) +
        '<div class="card" style="margin:16px"><h2>Offline</h2><p class="empty">You\'re offline. The desk will reconnect when the network returns.</p><button class="retry" type="button" data-action="reload-home">Retry</button></div>';
    }
    if (state.dashState === "error" && !state.dashboard) {
      return screenHead(greeting() + ", Shavor", connectionPill()) +
        '<div class="card" style="margin:16px"><h2>Couldn\'t load Home</h2><p class="empty">' + esc(state.dashError) + '</p><button class="retry" type="button" data-action="reload-home">Retry</button></div>';
    }
    const data = state.dashboard || {};
    const goal = data.goal || {};
    let html = screenHead(greeting() + ", Shavor", connectionPill());
    if (state.offline) {
      html += '<p class="banner bad">You\'re offline. Showing the last Home response this page received.</p>';
    }
    if (data.priority) {
      html += '<section class="cards"><article class="card wide"><p class="label">Now</p><h2>' +
        esc(data.priority.title || "Needs a decision") + "</h2><p>" + esc(data.priority.detail || "") + "</p></article></section>";
    }
    html += '<section class="cards">';
    html += card("Today", tasksPreview(data.tasks), "tasks");
    html += card("Latest briefing", briefingPreview(data.briefing), "briefings");
    html += card("Desk status", deskStatus(data.desk), false);
    html += card("Active goal", goalBlock(goal), false);
    if (data.connected_apps) html += card("Connected apps", appsBlock(data.connected_apps), false);
    if (data.top_gainers) html += card("Top gainers", gainersPreview(data.top_gainers), false);
    html += '<article class="card wide"><div class="subhead"><h2>Recent</h2></div>' + recentBlock(data) + "</article>";
    html += "</section>";
    return html;
  }

  function screenHead(title, extra) {
    return '<header class="screen-head"><h1>' + esc(title) + '</h1><span class="grow"></span>' + (extra || "") + "</header>";
  }

  function card(title, body, route) {
    const open = route ? '<button class="list-btn" type="button" data-action="go" data-route="' +
      route + '"><span>Open</span></button>' : "";
    return '<article class="card"><div class="subhead"><h2>' + esc(title) + "</h2></div>" + body + open + "</article>";
  }

  function tasksPreview(tasks) {
    const rows = (tasks || []).slice(0, 3);
    if (!rows.length) {
      return '<p class="empty">No desk tasks yet. Nothing here is synced from another account.</p>';
    }
    return rows.map(function (task) {
      return '<p><b>' + esc(task.title) + "</b><br><span class=\"meta\">" + esc(task.owner || "") +
        " · " + esc(task.due_label || "No due time") + "</span></p>";
    }).join("");
  }

  function briefingPreview(item) {
    if (!item) return '<p class="empty">No briefing yet. One appears only after the desk files it with a source and a time.</p>';
    return "<h3>" + esc(item.title) + "</h3><p class=\"meta\">" + esc(fmtTs(item.published_at || item.retrieved_at)) +
      "</p><p>" + esc(item.summary || "") + "</p>";
  }

  function deskStatus(desk) {
    desk = desk || {};
    return ["ace", "rail", "anchor"].map(function (key) {
      const row = desk[key] || { state: "unknown", detail: "No status reported." };
      const label = key === "ace" ? "Ace" : key === "rail" ? "Rail" : "Anchor";
      return '<div class="person"><b>' + label + '</b><span class="state-' + esc(row.state) + '">' +
        esc(row.state) + "</span></div><p class=\"meta\">" + esc(row.detail || "") + "</p>";
    }).join("");
  }

  function goalBlock(goal) {
    let html = "<p>" + esc(goal.title || "No active goal on file.") + "</p>";
    if (goal.progress) {
      html += '<p class="meta">Verified ' + esc(fmtTs(goal.progress.updated_at)) + ": " + esc(goal.progress.detail) + "</p>";
    } else {
      html += '<p class="empty">No verified progress snapshot yet.</p>';
    }
    return html;
  }

  function appsBlock(apps) {
    return (apps.items || []).map(function (item) {
      return '<button class="app-row" type="button" data-action="open-app" data-arg="' + esc(item.id) + '"><span><b>' +
        esc(item.name) + '</b><small>' + esc(item.state_label || item.state) + "</small></span></button>";
    }).join("") || '<p class="empty">No connected apps reported.</p>';
  }

  function gainersPreview(block) {
    const asOf = block.retrieved_at ? "As of " + fmtTs(block.retrieved_at) : "No retrieval time";
    let html = '<p class="meta">' + esc(block.source || "combined") + " · " + esc(asOf) + "</p>";
    const parts = block.parts || {};
    ["moomoo", "webull"].forEach(function (name) {
      if (!parts[name]) return;
      html += '<p class="meta">' + esc(name) + ": " + esc(parts[name].state) + "</p>";
    });
    (block.rows || []).slice(0, 3).forEach(function (row) {
      html += "<p><b>" + esc(row.ticker) + "</b> " + esc(row.change_pct) +
        '% <span class="meta">' + esc(row.source) + "</span></p>";
    });
    if (!block.rows || !block.rows.length) {
      html += '<p class="empty">' + esc(block.detail || block.empty || "No verified gainers.") + "</p>";
    }
    html += '<button class="list-btn" type="button" data-action="open-markets"><span>Open markets</span></button>';
    return html;
  }

  function recentBlock(data) {
    const items = data.recent || [];
    const decisions = data.decisions || [];
    if (!items.length && !decisions.length) {
      return '<p class="empty">No recent conversation, file, or decision.</p>';
    }
    let html = items.map(function (item) {
      const who = NAMES[item.from] || item.from || "Desk";
      return '<button class="list-btn" type="button" data-action="go" data-route="desk"><span><b>' +
        esc(item.kind === "file" ? "File" : "Conversation") + "</b><small>" + esc(who) + " · " +
        esc(item.text || item.attachment_kind || "") + "</small></span></button>";
    }).join("");
    html += decisions.map(function (item) {
      return '<button class="list-btn" type="button" data-action="go" data-route="desk"><span><b>Decision</b><small>' +
        esc(item.title || "") + " · " + esc(item.status || "") + "</small></span></button>";
    }).join("");
    return html;
  }

  function deskScreen() {
    let html = '<header class="topbar"><div><p class="label" style="margin:0">Thread</p><h1>Desk</h1></div>' +
      connectionPill() +
      '<button class="iconbtn" type="button" data-action="toggle-search" aria-label="Search the desk">Search</button>' +
      '<button class="spade-btn" type="button" data-action="open-desk-menu" aria-label="More desk options">' + spade() + "</button></header>";
    html += '<button class="mode-chip" type="button" data-action="open-mode" style="margin:0 16px 8px;width:auto">' +
      (state.mode === "panel" ? "Panel" : "One answer") + "</button>";
    if (state.searchOpen) {
      html += '<div style="padding:0 16px 8px"><input id="search" type="text" placeholder="Search this thread" value="' +
        esc(state.search) + '" aria-label="Search this thread" style="width:100%;min-height:48px;border:0;border-radius:14px;background:var(--surface-1);padding:0 14px"></div>';
    }
    if (state.contextOpen && state.dashboard && state.dashboard.goal) {
      const goal = state.dashboard.goal;
      html += '<section class="context"><p class="label">Context</p><p><b>Goal.</b> ' + esc(goal.title) +
        "</p><p><b>Lock.</b> " + esc(goal.lock) + "</p><p><b>Risk.</b> " + esc(goal.risk) +
        "</p><p>" + esc(goal.notes) + "</p></section>";
    }
    if (!state.connected && !state.messages.length) {
      html += '<p class="banner">Loading the desk…</p>';
    }
    html += '<div class="log" id="log">' + groupsHtml() + progressHtml() + "</div>";
    html += composer();
    return html;
  }

  function groupsHtml() {
    const query = state.search.trim().toLowerCase();
    const groups = [];
    let current = null;
    state.messages.forEach(function (message, index) {
      if (message.hidden) return;
      if (message.from === "shavor") {
        current = { q: message, qIndex: index, answers: [] };
        groups.push(current);
      } else if (!current) {
        current = { q: null, qIndex: -1, answers: [{ message: message, index: index }] };
        groups.push(current);
      } else {
        current.answers.push({ message: message, index: index });
      }
    });
    const visible = groups.filter(function (group) {
      if (!query) return true;
      const parts = [];
      if (group.q) parts.push(group.q.text || "");
      group.answers.forEach(function (item) { parts.push(item.message.text || ""); });
      return parts.join(" ").toLowerCase().indexOf(query) >= 0;
    });
    if (!visible.length) {
      return '<p class="empty">' + (query ? "Nothing in this thread matches." : "The desk is quiet.") + "</p>";
    }
    return visible.map(function (group) {
      let html = '<section class="group">';
      if (group.q) html += bubble(group.q, group.qIndex);
      group.answers.forEach(function (item) {
        html += item.message.from === "ace" && state.mode !== "panel"
          ? answerCard(item.message, item.index)
          : bubble(item.message, item.index);
      });
      html += "</section>";
      return html;
    }).join("");
  }

  function bubble(message, index) {
    const who = message.from === "shavor" ? "me" : "partner";
    const name = NAMES[message.from] || message.from;
    return '<article class="msg ' + who + '" data-msg="' + index + '"><div class="who ' + esc(message.from) + '">' +
      esc(name) + ' <span class="meta">' + esc(fmtTs(message.ts)) + "</span></div><div>" +
      esc(message.text || "") + attachmentHtml(message.attachment) + "</div></article>";
  }

  function answerCard(message, index) {
    const sections = message.structured || { answer: message.text || "" };
    let html = '<article class="answer" data-msg="' + index + '"><div class="who ace">Ace <span class="meta">' +
      esc(fmtTs(message.ts)) + "</span></div>";
    if (message.agreement === "agrees") html += '<p class="meta">Desk agrees</p>';
    if (message.agreement === "split") html += '<p class="meta">Split view</p>';
    if (message.agreement === "missing") html += '<p class="meta">Missing voice</p>';
    html += sectionBlock("Answer", sections.answer || message.text || "");
    if (sections.why) html += sectionBlock("Why", sections.why);
    if (sections.action) html += sectionBlock("Action", Array.isArray(sections.action) ? sections.action.map(function (step, i) {
      return (i + 1) + ". " + step;
    }).join("\n") : sections.action);
    if (sections.risk) html += sectionBlock("Risk", sections.risk);
    if (sections.sources) html += sectionBlock("Sources", sections.sources);
    if (sections.desk_notes || message.desk_notes) html += sectionBlock("Desk notes", sections.desk_notes || message.desk_notes);
    html += attachmentHtml(message.attachment) + "</article>";
    return html;
  }

  function sectionBlock(title, body) {
    if (!body) return "";
    return "<h3>" + esc(title) + "</h3><p>" + esc(body) + "</p>";
  }

  function attachmentHtml(attachment) {
    if (!attachment || !attachment.url) return "";
    const src = esc(attachment.url) + (state.token ? ("?token=" + encodeURIComponent(state.token)) : "");
    if (attachment.kind === "video") {
      return '<video class="clip" src="' + src + '" controls playsinline></video>';
    }
    return '<a href="' + src + '" target="_blank" rel="noopener"><img class="shot" alt="attachment" src="' + src + '"></a>';
  }

  function progressHtml() {
    const phase = state.requestState;
    const active = phase === "accepted" || phase === "researching" || phase === "partner_ready" ||
      phase === "synthesizing" || phase === "partial" || Object.keys(state.thinking).some(function (key) {
        const value = state.thinking[key];
        return value === "thinking" || value === "reacting";
      });
    if (!active || phase === "complete" || phase === "failed") return "";
    if (state.mode !== "panel") {
      if (phase === "partial") return '<p class="progress">One partner is missing. Ace is synthesizing.</p>';
      if (phase === "synthesizing") return '<p class="progress">Ace is synthesizing.</p>';
      return '<p class="progress">Rail and Anchor are reviewing; Ace is synthesizing.</p>';
    }
    const reacting = Object.keys(state.thinking).some(function (key) {
      return state.thinking[key] === "reacting";
    });
    return '<p class="progress">' + (reacting ? "Rail and Anchor are reacting." : "Rail and Anchor are answering.") + "</p>";
  }

  function composer() {
    return '<form class="composer" id="composer">' +
      '<input id="input" type="text" autocomplete="off" placeholder="Ask the desk" value="' + esc(state.draft) + '" aria-label="Message">' +
      '<label id="attachBtn" class="iconbtn" for="fileInput" title="Send a photo or video" aria-label="Send a photo or video">＋</label>' +
      '<input id="fileInput" class="filehidden" type="file" accept="image/*,video/*">' +
      '<button id="micBtn" class="iconbtn" type="button" data-action="mic" aria-label="Dictate">🎤</button>' +
      '<button id="send" type="submit"' + (state.connected ? "" : " disabled") + ">Send</button></form>";
  }

  function tasksScreen() {
    let html = screenHead("Tasks", "");
    html += '<div class="segments" role="tablist">' + TASK_VIEWS.map(function (view) {
      return '<button type="button" data-action="task-view" data-arg="' + view[0] + '" aria-pressed="' +
        (state.taskView === view[0] ? "true" : "false") + '">' + view[1] + "</button>";
    }).join("") + "</div>";
    html += '<section class="stack">';
    if (state.taskError) {
      html += '<p class="banner bad">Couldn\'t load tasks.</p><button class="retry" type="button" data-action="reload-tasks">Retry</button>';
    } else if (!state.tasks.length) {
      html += '<article class="card"><h2>Desk tasks</h2><p class="empty">No desk tasks in this view. Nothing is synced from another account.</p></article>';
    } else {
      state.tasks.forEach(function (task) {
        const done = task.status === "completed";
        html += '<button class="task-row' + (done ? " done" : "") + '" type="button" data-action="complete-task" data-arg="' +
          esc(task.id) + '"><span class="check" aria-hidden="true">' + (done ? "✓" : "") + '</span><span><b class="title">' +
          esc(task.title) + "</b><small>" + esc(task.owner) + " · " + esc(task.due_at || "No due time") +
          "</small></span></button>";
      });
    }
    html += "</section>";
    return html;
  }

  function briefingsScreen() {
    let html = screenHead("Briefings", "");
    html += '<div class="segments" role="tablist">' + BRIEF_TABS.map(function (tab) {
      return '<button type="button" data-action="brief-tab" data-arg="' + tab[0] + '" aria-pressed="' +
        (state.briefTab === tab[0] ? "true" : "false") + '">' + tab[1] + "</button>";
    }).join("") + "</div><section class=\"stack\">";
    if (state.briefTab === "markets") html += gainersModule();
    const rows = state.briefings.filter(function (item) {
      if (state.briefTab === "for_you") return true;
      return item.category === state.briefTab;
    });
    if (!rows.length) {
      html += '<article class="card"><h2>Nothing filed</h2><p class="empty">No briefing in this tab. Lines appear only from a verified feed, a saved desk brief, or a sourced provider response.</p></article>';
    }
    rows.forEach(function (item) {
      html += '<article class="card"><div class="subhead"><h2>' + esc(item.title) + "</h2>" +
        (item.pinned ? '<span class="pill">Pinned</span>' : "") + "</div><p class=\"meta\">" +
        esc(item.provenance === "desk_analysis" ? "Desk analysis" : "Source") + " · published " +
        esc(fmtTs(item.published_at) || "unknown") + " · retrieved " + esc(fmtTs(item.retrieved_at)) +
        "</p><p>" + esc(item.summary || "") + "</p>" +
        '<button class="list-btn" type="button" data-action="ask-brief" data-arg="' + esc(item.title) +
        '"><span>Ask the desk about this</span></button></article>';
    });
    html += "</section>";
    return html;
  }

  function gainersModule() {
    const block = state.gainers;
    let html = '<article class="card"><div class="subhead"><h2>Top gainers</h2></div>';
    html += '<div class="segments" style="padding:0 0 8px">';
    ["combined", "moomoo", "webull"].forEach(function (source) {
      const label = source === "webull" ? "Webull" : source === "moomoo" ? "moomoo" : "Combined";
      html += '<button type="button" data-action="gainer-source" data-arg="' + source + '" aria-pressed="' +
        (state.gainerSource === source ? "true" : "false") + '">' + label + "</button>";
    });
    html += "</div>";
    if (!block) {
      html += '<p class="empty">Loading source state…</p></article>';
      return html;
    }
    html += '<p class="meta">' + esc(block.source) + " · " +
      (block.retrieved_at ? "as of " + esc(fmtTs(block.retrieved_at)) : "no retrieval time") + "</p>";
    const parts = block.parts || {};
    Object.keys(parts).forEach(function (name) {
      html += '<p class="meta">' + esc(name) + ": " + esc(parts[name].state) +
        (parts[name].detail ? " — " + esc(parts[name].detail) : "") + "</p>";
    });
    if (!block.rows || !block.rows.length) {
      html += '<p class="empty">No verified rows for this source.</p>';
    }
    (block.rows || []).forEach(function (row) {
      html += '<button class="gainer-row" type="button" data-action="ask-ticker" data-arg="' + esc(row.ticker) +
        '"><span><b>' + esc(row.rank) + " " + esc(row.ticker) + "</b><small>" + esc(row.last) + " · " +
        esc(row.change_pct) + "% · " + esc(row.session || "session unknown") + " · " + esc(row.source) +
        "</small></span></button>";
    });
    html += '<p class="meta">A ticker opens a question for the desk. It does not place an order.</p></article>';
    return html;
  }

  function appsScreen() {
    const focus = state.routeArg;
    if (focus) return appDetail(focus);
    const apps = (state.integrations && state.integrations.apps) || [];
    let html = screenHead("Apps and Bluetooth", "");
    html += '<section class="stack">';
    if (!apps.length) html += '<p class="banner">Loading connection state…</p>';
    apps.forEach(function (item) {
      const installed = nativeInstall(item.id);
      let label = item.account === "connected" ? "Connected" : "Not connected";
      if (installed === false) label = "Unavailable";
      else if (installed === true && item.account !== "connected") label = "Installed · not connected";
      html += '<button class="app-row" type="button" data-action="open-app" data-arg="' + esc(item.id) +
        '"><span><b>' + esc(item.name) + "</b><small>" + esc(label) + "</small></span></button>";
    });
    html += '<article class="card"><h2>Bluetooth</h2>' + bluetoothBlock() + "</article></section>";
    return html;
  }

  function appDetail(id) {
    const apps = (state.integrations && state.integrations.apps) || [];
    const item = apps.filter(function (row) { return row.id === id; })[0] || { id: id, name: id, account: "not_connected", detail: "" };
    const installed = nativeInstall(id);
    let html = screenHead(item.name || id, "");
    html += '<section class="stack"><article class="card"><p class="meta">' + esc(item.detail || "No account is connected.") + "</p>";
    if (installed === false) html += '<p class="empty">The app is not installed on this device. You can still open the verified web destination.</p>';
    if (item.account !== "connected") html += '<p class="empty">No feed is shown. Account content appears only after an authenticated capability check.</p>';
    html += '<button class="list-btn" type="button" data-action="launch-app" data-arg="' + esc(id) + '"><span>Open ' + esc(item.name || id) + "</span></button>";
    html += '<button class="list-btn" type="button" data-action="ask-app" data-arg="' + esc(item.name || id) + '"><span>Ask the desk</span></button>';
    html += "</article></section>";
    return html;
  }

  function nativeInstall(id) {
    try {
      if (window.DeskNative && DeskNative.appInstallState) {
        const parsed = JSON.parse(DeskNative.appInstallState());
        if (parsed && typeof parsed[id] === "boolean") return parsed[id];
      }
    } catch (e) {}
    return null;
  }

  function bluetoothBlock() {
    let info = null;
    try {
      if (window.DeskNative && DeskNative.bluetoothState) info = JSON.parse(DeskNative.bluetoothState());
    } catch (e) {}
    if (!info || !info.state) {
      return '<p class="empty">Bluetooth follows the phone. Open Desk on Android to read the adapter. This page will not pretend a switch changed it.</p>';
    }
    const on = info.state === "on";
    return '<button class="list-btn" type="button" data-action="bluetooth" aria-pressed="' + (on ? "true" : "false") +
      '"><span><b>Bluetooth ' + (on ? "on" : "off") + "</b><small>Opens system Bluetooth settings. The label updates when you come back.</small></span></button>";
  }

  function moreScreen() {
    const data = state.dashboard || {};
    const goal = data.goal || {};
    const desk = data.desk || {};
    let html = screenHead("More", "");
    html += '<section class="stack">';
    html += '<article class="card"><h2>Shared memory</h2><p class="meta">' +
      (goal.updated_at ? "Updated " + esc(fmtTs(goal.updated_at)) : "No memory update on file.") +
      "</p></article>";
    html += '<article class="card"><h2>Standing rules</h2><p>' + esc(goal.lock || "No lock on file.") +
      "</p><p class=\"meta\" style=\"margin-top:8px\">" + esc(goal.risk || "") + "</p></article>";
    html += '<article class="card"><h2>Provider health</h2>' + deskStatus(desk) + "</article>";
    html += '<article class="card"><h2>Apps and Bluetooth</h2><button class="list-btn" type="button" data-action="go" data-route="apps"><span>YouTube, Facebook, Snapchat, Bluetooth</span></button></article>';
    html += '<article class="card"><h2>Voice</h2><button class="list-btn" type="button" data-action="speak-toggle"><span>' +
      (state.speakOn ? "Speaker on" : "Speaker off") + "<small>Reads new replies on this device.</small></span></button></article>";
    html += '<article class="card"><h2>Versions</h2><p class="meta" id="versions">' + esc(state.versionLabel || "Loading versions…") + "</p>" +
      '<button class="retry" type="button" data-action="refresh-bundle">Check for update</button></article>';
    html += '<article class="card"><button class="list-btn" type="button" data-action="sign-out"><span>Sign out</span></button></article>';
    html += "</section>";
    return html;
  }

  function gate() {
    return '<div class="shell"><form class="gate card" id="gate"><h1>' + spade() + ' Desk</h1>' +
      '<p class="muted">Paste the room token once. It stays in this browser so the next open can exchange it.</p>' +
      '<input id="gtok" type="text" autocomplete="off" placeholder="Room token" aria-label="Room token">' +
      '<button type="submit">Join the desk</button></form></div>';
  }

  function render() {
    if (state.signedOut || (!state.token && !state.authed)) {
      app.innerHTML = gate();
      return;
    }
    const focusId = document.activeElement && document.activeElement.id;
    const caret = focusId && document.activeElement.selectionStart;
    const screens = { home: homeScreen, desk: deskScreen, tasks: tasksScreen, briefings: briefingsScreen, more: moreScreen, apps: appsScreen };
    app.innerHTML = shell((screens[state.route] || homeScreen)());
    if (focusId) {
      const field = document.getElementById(focusId);
      if (field) {
        field.focus();
        if (caret != null && field.setSelectionRange) field.setSelectionRange(caret, caret);
      }
    }
    const log = document.getElementById("log");
    if (log) log.scrollTop = log.scrollHeight;
    if (state.route === "more") loadVersions();
  }

  let versionsLoading = false;
  async function loadVersions() {
    if (state.versionLabel || versionsLoading) return;
    versionsLoading = true;
    try {
      const response = await api("/api/v2/version");
      const body = await response.json();
      state.versionLabel = "Frontend " + (body.frontend || "—") + " · Backend " + (body.backend || "—") +
        " · Protocol " + (body.protocol || "—");
    } catch (e) {
      state.versionLabel = "Versions unavailable.";
    }
    versionsLoading = false;
    if (state.route === "more") {
      const node = document.getElementById("versions");
      if (node) node.textContent = state.versionLabel;
    }
  }

  function sendPayload(extra) {
    return Object.assign({
      type: "user",
      text: state.draft.trim(),
      crosstalk: state.mode === "panel" && state.crosstalk,
      funnel: state.mode !== "panel",
      message_id: uuid(),
      thread_id: "desk",
      reply_to: state.replyTo || undefined,
    }, extra || {});
  }

  function send() {
    const text = state.draft.trim();
    if (!text || !ws || ws.readyState !== 1) return;
    ws.send(JSON.stringify(sendPayload()));
    state.draft = "";
    state.replyTo = null;
    const field = document.getElementById("input");
    if (field) field.value = "";
  }

  async function onFile(file) {
    if (!file) return;
    if (!ws || ws.readyState !== 1) {
      window.addMsg("me", "Not connected — try again in a moment.");
      return;
    }
    const label = document.getElementById("attachBtn");
    if (label) label.textContent = "…";
    try {
      const body = new FormData();
      body.append("file", file);
      const uploadUrl = "/api/upload" + (state.token ? ("?token=" + encodeURIComponent(state.token)) : "");
      const response = await fetch(uploadUrl, { method: "POST", body: body, credentials: "include" });
      const payload = await response.json();
      if (!payload.ok) throw new Error(payload.error || "upload failed");
      ws.send(JSON.stringify(sendPayload({
        attachment: { url: payload.url, kind: payload.kind, name: payload.name },
      })));
      state.draft = "";
    } catch (error) {
      window.addMsg("me", "Couldn't send that file: " + error.message);
    }
    const again = document.getElementById("attachBtn");
    if (again) again.textContent = "＋";
  }

  window.addMsg = function (who, text) {
    state.messages.push({
      from: who === "me" ? "shavor" : who,
      text: text,
      ts: new Date().toISOString(),
    });
    if (state.route === "desk") render();
    return "shown";
  };

  window.deskSendAttachment = function (url, kind, name) {
    try {
      if (!ws || ws.readyState !== 1) return "offline";
      ws.send(JSON.stringify(sendPayload({
        attachment: { url: url, kind: kind, name: name },
      })));
      state.draft = "";
      return "sent";
    } catch (error) {
      return "error:" + (error && error.message ? error.message : "send failed");
    }
  };

  function rememberIncoming(message) {
    const id = message.id || message.message_id;
    if (id) {
      for (let i = 0; i < state.messages.length; i++) {
        const current = state.messages[i].id || state.messages[i].message_id;
        if (current === id) {
          state.messages[i] = Object.assign(state.messages[i], message);
          return;
        }
      }
    }
    state.messages.push(message);
  }

  function connect() {
    if (!state.token && !state.authed) return;
    const gen = ++socketGen;
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const query = state.token ? ("?token=" + encodeURIComponent(state.token)) : "";
    ws = new WebSocket(proto + "://" + location.host + "/ws" + query);
    ws.onopen = function () {
      if (gen !== socketGen) return;
      state.connected = true;
      state.offline = false;
      render();
    };
    ws.onclose = function () {
      if (gen !== socketGen) return;
      state.connected = false;
      render();
      setTimeout(function () { if (gen === socketGen) connect(); }, 2500);
    };
    ws.onmessage = function (event) {
      if (gen !== socketGen) return;
      const message = JSON.parse(event.data);
      if (message.type === "history") {
        state.messages = message.messages || [];
        state.thinking = {};
        state.requestState = "";
        if (state.route === "desk") render();
        return;
      } else if (message.type === "ack") {
        state.requestState = message.state || "accepted";
      } else if (message.type === "request_state") {
        state.requestState = message.state || "";
        if (message.state === "complete" || message.state === "failed") state.thinking = {};
      } else if (message.type === "user") {
        rememberIncoming({
          id: message.message_id,
          from: "shavor",
          text: message.text,
          ts: message.ts,
          attachment: message.attachment,
          reply_to: message.reply_to,
        });
      } else if (message.type === "status") {
        state.thinking[message.provider] = message.state;
        if (message.state === "error") {
          state.messages.push({
            from: message.provider,
            text: message.detail || "Request failed",
            ts: new Date().toISOString(),
          });
        }
      } else if (message.type === "reply") {
        state.thinking[message.provider] = "done";
        rememberIncoming({
          id: message.message_id,
          from: message.provider,
          text: message.text,
          ts: message.ts,
          round: message.round,
          attachment: message.attachment,
          structured: message.structured,
          agreement: message.agreement,
          desk_notes: message.desk_notes,
        });
        speak(message.provider, message.text);
      }
      if (state.route === "desk") render();
    };
  }

  const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
  let recognition = null;
  function toggleMic() {
    if (!SR) return;
    if (recognition) {
      try { recognition.stop(); } catch (e) {}
      recognition = null;
      return;
    }
    recognition = new SR();
    recognition.lang = "en-US";
    recognition.onresult = function (event) {
      const text = event.results[0][0].transcript || "";
      state.draft = (state.draft ? state.draft + " " : "") + text;
      render();
    };
    recognition.onend = function () { recognition = null; };
    try { recognition.start(); } catch (e) { recognition = null; }
  }

  function speak(who, text) {
    if (!state.speakOn || !text || !window.speechSynthesis) return;
    try {
      const utterance = new SpeechSynthesisUtterance(text);
      utterance.pitch = who === "grok" ? 0.8 : who === "ace" ? 1.15 : 1;
      window.speechSynthesis.cancel();
      window.speechSynthesis.speak(utterance);
    } catch (e) {}
  }

  function openDeskMenu() {
    state.menu = {
      label: "More desk options",
      actions: [
        ["toggle-search", "Search"],
        ["toggle-context", state.contextOpen ? "Hide context" : "Context"],
        ["open-mode", "Answer mode"],
        ["speak-toggle", state.speakOn ? "Mute speaker" : "Read replies aloud"],
        ["sign-out", "Sign out"],
        ["close-menu", "Close"],
      ],
    };
    render();
  }

  function openMode() {
    state.menu = {
      label: "Answer mode",
      actions: [
        ["set-mode", "One answer", "one_answer"],
        ["set-mode", "Panel", "panel"],
        ["toggle-xtalk", state.crosstalk ? "Cross-talk on" : "Cross-talk off"],
        ["close-menu", "Close"],
      ],
    };
    render();
  }

  function openMessageMenu(index) {
    const message = state.messages[index];
    if (!message) return;
    state.menu = {
      label: "Message",
      actions: [
        ["copy-msg", "Copy", String(index)],
        ["share-msg", "Share", String(index)],
        ["reply-msg", "Reply", String(index)],
        ["pin-msg", "Pin", String(index)],
        ["retry-msg", "Retry", String(index)],
        ["task-msg", "Turn into task", String(index)],
        ["close-menu", "Close"],
      ],
    };
    render();
  }

  async function copyMessage(index) {
    const message = state.messages[index];
    if (!message) return;
    const text = message.text || "";
    try {
      await navigator.clipboard.writeText(text);
    } catch (e) {}
    state.menu = null;
    render();
  }

  async function shareMessage(index) {
    const message = state.messages[index];
    if (!message) return;
    const text = message.text || "";
    try {
      if (navigator.share) await navigator.share({ text: text });
      else await navigator.clipboard.writeText(text);
    } catch (e) {}
    state.menu = null;
    render();
  }

  function onClick(event) {
    if (event.target.classList && event.target.classList.contains("sheet-back")) {
      state.menu = null;
      render();
      return;
    }
    const node = event.target.closest("[data-action]");
    if (!node) return;
    const action = node.dataset.action;
    const arg = node.dataset.arg;
    if (action === "go") go(node.dataset.route);
    else if (action === "close-menu") { state.menu = null; render(); }
    else if (action === "open-desk-menu") openDeskMenu();
    else if (action === "open-mode") openMode();
    else if (action === "toggle-search") { state.searchOpen = !state.searchOpen; state.menu = null; render(); }
    else if (action === "toggle-context") {
      state.contextOpen = !state.contextOpen;
      state.menu = null;
      if (state.contextOpen && !state.dashboard) loadDashboard();
      render();
    } else if (action === "set-mode") {
      state.mode = arg === "panel" ? "panel" : "one_answer";
      try { localStorage.setItem("desk_mode", state.mode); } catch (e) {}
      state.menu = null;
      render();
    } else if (action === "toggle-xtalk") {
      state.crosstalk = !state.crosstalk;
      try { localStorage.setItem("desk_xtalk", state.crosstalk ? "1" : "0"); } catch (e) {}
      openMode();
    } else if (action === "speak-toggle") {
      state.speakOn = !state.speakOn;
      try { localStorage.setItem("desk_speak", state.speakOn ? "1" : "0"); } catch (e) {}
      if (!state.speakOn && window.speechSynthesis) window.speechSynthesis.cancel();
      state.menu = null;
      render();
    } else if (action === "mic") toggleMic();
    else if (action === "reload-home") loadDashboard();
    else if (action === "refresh-bundle") location.reload();
    else if (action === "sign-out") {
      try { localStorage.removeItem("desk_token"); } catch (e) {}
      state.signedOut = true;
      state.token = "";
      socketGen += 1;
      if (ws) try { ws.close(); } catch (e) {}
      history.replaceState({}, "", location.pathname);
      render();
    }     else if (action === "task-view") { state.taskView = arg; loadTasks(); }
    else if (action === "brief-tab") { state.briefTab = arg; if (arg === "markets") loadGainers(); render(); }
    else if (action === "reload-tasks") loadTasks();
    else if (action === "open-markets") { state.briefTab = "markets"; go("briefings"); }
    else if (action === "gainer-source") { state.gainerSource = arg; loadGainers(); }
    else if (action === "open-app") go("apps", arg);
    else if (action === "launch-app") launchApp(arg);
    else if (action === "ask-app") { state.draft = "Look at " + arg + " with me."; go("desk"); }
    else if (action === "ask-brief") { state.draft = "About this briefing: " + arg; go("desk"); }
    else if (action === "ask-ticker") { state.draft = "What should I know about " + arg + "? Read only — do not place an order."; go("desk"); }
    else if (action === "complete-task") completeTask(arg);
    else if (action === "undo-task") undoTask();
    else if (action === "bluetooth") openBluetooth();
    else if (action === "task-msg") turnIntoTask(Number(arg));
    else if (action === "copy-msg") copyMessage(Number(arg));
    else if (action === "share-msg") shareMessage(Number(arg));
    else if (action === "reply-msg") {
      const message = state.messages[Number(arg)];
      state.replyTo = message && (message.id || message.message_id) || null;
      state.menu = null;
      render();
    } else if (action === "pin-msg") pinMessage(Number(arg));
    else if (action === "retry-msg") retryMessage(Number(arg));
    else if (action === "open-app") go("more");
  }

  app.addEventListener("click", onClick);
  app.addEventListener("submit", function (event) {
    if (event.target.id === "composer") {
      event.preventDefault();
      send();
    }
    if (event.target.id === "gate") {
      event.preventDefault();
      const value = document.getElementById("gtok").value.trim();
      if (!value) return;
      fetch("/api/v2/session", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token: value }),
      }).then(function (response) {
        if (!response.ok) return;
        location.href = location.pathname + "#/home";
      });
    }
  });
  app.addEventListener("input", function (event) {
    if (event.target.id === "input") state.draft = event.target.value;
    if (event.target.id === "search") {
      state.search = event.target.value;
      const log = document.getElementById("log");
      if (log) {
        const focus = document.activeElement;
        const caret = focus && focus.selectionStart;
        log.innerHTML = groupsHtml() + progressHtml();
        if (focus && focus.id === "search") {
          const field = document.getElementById("search");
          if (field) {
            field.focus();
            if (caret != null) field.setSelectionRange(caret, caret);
          }
        }
      }
    }
  });
  app.addEventListener("change", function (event) {
    if (event.target.id === "fileInput") {
      const file = event.target.files && event.target.files[0];
      event.target.value = "";
      onFile(file);
    }
  });
  app.addEventListener("keydown", function (event) {
    if (event.target.id === "input" && event.key === "Enter") {
      event.preventDefault();
      send();
    }
  });
  app.addEventListener("pointerdown", function (event) {
    const node = event.target.closest("[data-msg]");
    if (!node) return;
    const index = node.dataset.msg;
    pressTimer = setTimeout(function () { openMessageMenu(index); }, 550);
  });
  app.addEventListener("pointerup", function () { clearTimeout(pressTimer); });
  app.addEventListener("pointercancel", function () { clearTimeout(pressTimer); });

  window.addEventListener("hashchange", function () {
    state.route = routeFromHash();
    render();
    loadRouteData();
  });
  window.addEventListener("popstate", function () {
    state.route = routeFromHash();
    render();
    loadRouteData();
  });
  window.addEventListener("offline", function () {
    state.offline = true;
    render();
  });
  window.addEventListener("online", function () {
    state.offline = false;
    if (!state.connected) connect();
    render();
  });

  function watchViewport() {
    const viewport = window.visualViewport;
    if (!viewport) return;
    const apply = function () {
      const inset = Math.max(0, window.innerHeight - viewport.height - viewport.offsetTop);
      document.documentElement.style.setProperty("--kb", inset + "px");
    };
    viewport.addEventListener("resize", apply);
    viewport.addEventListener("scroll", apply);
    apply();
  }

  async function loadTasks() {
    try {
      const response = await api("/api/v2/tasks?view=" + encodeURIComponent(state.taskView));
      if (!response.ok) throw new Error("tasks");
      state.tasks = (await response.json()).tasks || [];
      state.taskError = false;
    } catch (e) {
      state.taskError = true;
    }
    if (state.route === "tasks") render();
  }

  async function loadBriefings() {
    try {
      const response = await api("/api/v2/briefings");
      if (!response.ok) throw new Error("briefings");
      state.briefings = (await response.json()).briefings || [];
    } catch (e) {
      state.briefings = [];
    }
    if (state.route === "briefings") render();
  }

  async function loadGainers() {
    try {
      const response = await api("/api/v2/market/top-gainers?source=" + encodeURIComponent(state.gainerSource));
      if (!response.ok) throw new Error("gainers");
      state.gainers = await response.json();
    } catch (e) {
      state.gainers = { source: state.gainerSource, state: "error", rows: [], detail: "Couldn't load gainers.", parts: {} };
    }
    if (state.route === "briefings" || state.route === "home") render();
  }

  async function loadIntegrations() {
    try {
      const response = await api("/api/v2/integrations");
      if (!response.ok) throw new Error("integrations");
      state.integrations = await response.json();
    } catch (e) {
      state.integrations = { apps: [] };
    }
    if (state.route === "apps") render();
  }

  async function completeTask(id) {
    const task = state.tasks.filter(function (item) { return item.id === id; })[0];
    if (!task || task.status === "completed") return;
    const response = await api("/api/v2/tasks/" + encodeURIComponent(id) + "/complete", { method: "POST" });
    if (!response.ok) return;
    state.undo = task;
    await loadTasks();
    loadDashboard();
  }

  async function undoTask() {
    const task = state.undo;
    state.undo = null;
    if (!task) { render(); return; }
    await api("/api/v2/tasks/" + encodeURIComponent(task.id), {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status: "open" }),
    });
    await loadTasks();
    loadDashboard();
  }

  async function turnIntoTask(index) {
    const message = state.messages[index];
    state.menu = null;
    if (!message) { render(); return; }
    const title = (message.text || "Desk task").slice(0, 140);
    await api("/api/v2/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        title: title,
        source_message_id: message.id || message.message_id || null,
        thread_id: message.thread_id || "desk",
      }),
    });
    go("tasks");
  }

  async function launchApp(id) {
    try {
      if (window.DeskNative && DeskNative.openConnectedApp) {
        DeskNative.openConnectedApp(id);
        return;
      }
    } catch (e) {}
    const response = await api("/api/v2/integrations/" + encodeURIComponent(id) + "/launch", { method: "POST" });
    if (!response.ok) return;
    const target = await response.json();
    if (target.web) window.open(target.web, "_blank", "noopener");
  }

  function openBluetooth() {
    try {
      if (window.DeskNative && DeskNative.openBluetoothSettings) DeskNative.openBluetoothSettings();
    } catch (e) {}
  }

  window.deskBluetoothUpdated = function (raw) {
    state.bluetooth = raw;
    if (state.route === "apps") render();
  };

  async function pinMessage(index) {
    const message = state.messages[index];
    const id = message && (message.id || message.message_id);
    state.menu = null;
    if (!id) { render(); return; }
    await api("/api/v2/messages/" + encodeURIComponent(id) + "/pin", { method: "POST" });
    message.pinned = !message.pinned;
    render();
  }

  async function retryMessage(index) {
    const message = state.messages[index];
    const id = message && (message.id || message.message_id);
    state.menu = null;
    render();
    if (!id) return;
    await api("/api/v2/messages/" + encodeURIComponent(id) + "/retry", { method: "POST" });
  }

  async function establish() {
    const params = new URLSearchParams(location.search);
    let token = params.get("token") || "";
    try {
      if (!token) token = localStorage.getItem("desk_token") || "";
    } catch (e) {}
    if (token) {
      try {
        const response = await fetch("/api/v2/session", {
          method: "POST",
          credentials: "include",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ token: token }),
        });
        if (response.ok) {
          state.authed = true;
          state.token = "";
          try { localStorage.removeItem("desk_token"); } catch (e) {}
          if (params.get("token")) history.replaceState({ route: state.route }, "", location.pathname + location.hash);
          return;
        }
      } catch (e) {}
      state.token = token;
      state.authed = true;
      return;
    }
    try {
      const probe = await fetch("/api/v2/version", { credentials: "include", cache: "no-store" });
      state.authed = probe.ok;
    } catch (e) {
      state.authed = false;
    }
  }

  state.route = routeFromHash();
  if (!location.hash) history.replaceState({ route: "home" }, "", "#/home");
  watchViewport();
  app.innerHTML = '<p class="banner">Loading the desk…</p>';
  establish().then(function () {
    render();
    if (state.authed || state.token) {
      connect();
      loadDashboard();
      loadRouteData();
    }
  });
})();
