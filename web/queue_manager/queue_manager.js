import { app } from "/scripts/app.js";
import { api } from "/scripts/api.js";

const TAB_ID = "cherry-queue";
const STATE_EVENT = "cherry_queue.state";
const EXECUTION_EVENTS = ["execution_start", "execution_success", "execution_error", "execution_interrupted"];
const SAVE_MODE_KEY = "cherry-queue.save-mode";
const HOLD_KEY = "cherry_hold"; // must match HOLD_KEY in cherry_queue.py

function loadSaveMode() {
  try {
    return localStorage.getItem(SAVE_MODE_KEY) === "files" ? "files" : "list";
  } catch {
    return "list";
  }
}

// The server owns the queue; this is only the latest copy fetched from it, rebuilt after every F5.
const store = {
  jobs: [],
  paused: false,
  restore: null,
  labels: {},
  held: [],
  saved: [],
  progress: {},
  connected: true,
  error: null,
  saveMode: loadSaveMode(), // "list" = Saved list in the sidebar, "files" = workflow files; a per-browser preference
};

let view = null;
let refreshTimer = null;
let refreshSeq = 0;

const isRunning = (job) => job.status === "in_progress";
const plural = (count, word) => `${count} ${word}${count === 1 ? "" : "s"}`;

function h(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (value == null || value === false) continue;
    if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else if (key === "class") node.className = value;
    else if (key === "dataset") Object.assign(node.dataset, value);
    else node.setAttribute(key, value === true ? "" : value);
  }
  node.append(...children.filter((child) => child != null && child !== false && child !== ""));
  return node;
}

async function request(path, body) {
  const init = body === undefined ? undefined : {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
  const res = await api.fetchApi(path, init);
  const text = await res.text();
  let data = text;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    // non-JSON body (e.g. an HTML error page); keep the raw text
  }
  if (!res.ok) throw new Error(data?.error?.message ?? data?.error ?? `HTTP ${res.status}`);
  return data;
}

function scheduleRefresh(delay = 100) {
  clearTimeout(refreshTimer);
  refreshTimer = setTimeout(refresh, delay);
}

async function refresh() {
  const seq = ++refreshSeq;
  try {
    const [jobs, state] = await Promise.all([
      request("/jobs?status=in_progress,pending"),
      request("/cherry_queue/state"),
    ]);
    if (seq !== refreshSeq) return;
    store.jobs = sortJobs(jobs?.jobs ?? []);
    store.paused = state.paused;
    store.restore = state.restore;
    store.labels = state.labels ?? {};
    store.held = state.held ?? [];
    store.saved = state.saved ?? [];
    store.error = null;
    const runningIds = new Set(store.jobs.filter(isRunning).map((job) => job.id));
    for (const id of Object.keys(store.progress)) {
      if (!runningIds.has(id)) delete store.progress[id];
    }
  } catch (err) {
    if (seq !== refreshSeq) return;
    store.error = err.message;
  }
  view?.render();
}

function sortJobs(jobs) {
  return jobs.sort((a, b) =>
    Number(isRunning(b)) - Number(isRunning(a)) ||
    (a.priority ?? 0) - (b.priority ?? 0) ||
    (a.create_time ?? 0) - (b.create_time ?? 0));
}

function subscribe() {
  api.addEventListener("status", ({ detail }) => {
    const connected = detail != null;
    if (connected !== store.connected) {
      store.connected = connected;
      view?.render();
    }
    if (connected) scheduleRefresh();
  });
  api.addEventListener(STATE_EVENT, () => scheduleRefresh(0));
  for (const type of EXECUTION_EVENTS) api.addEventListener(type, () => scheduleRefresh());
  api.addEventListener("progress", ({ detail }) => {
    if (!detail?.prompt_id) return;
    store.progress[detail.prompt_id] = { value: detail.value, max: detail.max };
    view?.updateProgress();
  });
}

function toast(severity, summary, detail) {
  app.extensionManager.toast?.add({ severity, summary, detail, life: 5000 });
}

async function run(action) {
  try {
    await action();
  } catch (err) {
    toast("error", "Cherry Queue", err.message);
  }
  scheduleRefresh(0);
}

const setPaused = (paused) => request("/cherry_queue/pause", { paused, client_id: api.clientId });
const cancelJob = (id) => request(`/jobs/${id}/cancel`, {});
const moveJob = (id, before) => request("/cherry_queue/move", { id, before });
const dismissRestore = () => request("/cherry_queue/dismiss", {});
const renameJob = (id, name) => request("/cherry_queue/rename", { id, name });
const requeueSaved = (id) => request("/cherry_queue/saved/requeue", { id, client_id: api.clientId });
const renameSaved = (id, name) => request("/cherry_queue/saved/rename", { id, name });
const releaseHeld = (id) => request("/cherry_queue/held/release", { id, client_id: api.clientId });
const removeHeld = (id) => request("/cherry_queue/held/remove", { id });

async function downloadSaved(id) {
  const file = await request("/cherry_queue/saved/workflow", { id });
  downloadJson(file.filename, file.workflow);
}

async function restoreSaved(id) {
  const { filename, workflow } = await request("/cherry_queue/saved/workflow", { id });
  // If the name matches a saved workflow file, ComfyUI opens that file instead, so pick a name nothing uses yet.
  const base = filename.replace(/\.json$/i, "");
  const workflows = app.extensionManager.workflow;
  let name = base;
  for (let n = 2; workflows?.getWorkflowByPath?.(`workflows/${name}.json`); n++) name = `${base} (${n})`;
  await app.loadGraphData(workflow, true, true, name);
}

// Queues the current workflow exactly like the Run button (so seeds still advance and node errors still show),
// but tags that one /prompt request so the server puts the job on hold instead of in the queue.
async function queueOnHold() {
  if (app.processingQueue) throw new Error("A Run is still being sent to the server; try again in a moment");
  const hadOwn = Object.hasOwn(api, "fetchApi");
  const original = api.fetchApi;
  let tagged = false;
  const wrapper = function (route, options) {
    if (!tagged && route === "/prompt" && options?.method === "POST" && typeof options.body === "string") {
      tagged = true;
      const body = JSON.parse(options.body);
      body.extra_data = { ...body.extra_data, [HOLD_KEY]: true };
      options = { ...options, body: JSON.stringify(body) };
    }
    return original.call(this, route, options);
  };
  api.fetchApi = wrapper;
  try {
    await app.queuePrompt(0, 1);
  } finally {
    if (api.fetchApi === wrapper) {
      if (hadOwn) api.fetchApi = original;
      else delete api.fetchApi;
    }
  }
}

async function deleteSaved(id, name) {
  if (confirm(`Delete the saved job "${name}"?`)) await request("/cherry_queue/saved/delete", { id });
}

function downloadJson(filename, data) {
  const url = URL.createObjectURL(new Blob([JSON.stringify(data, null, 2)], { type: "application/json" }));
  const link = h("a", { href: url, download: filename });
  document.body.appendChild(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function saveJobs(ids) {
  const body = ids ? { ids } : { all: true };
  if (store.saveMode === "list") {
    const { saved } = await request("/cherry_queue/save_list", body);
    toast("success", `Saved ${plural(saved, "job")} to the Saved list`);
    return;
  }
  const { files, skipped } = await request("/cherry_queue/save_workflows", body);
  for (const [i, file] of files.entries()) {
    // Browsers drop some downloads when many start in the same instant, so space them out.
    if (i > 0) await new Promise((resolve) => setTimeout(resolve, 250));
    downloadJson(file.filename, file.workflow);
  }
  if (files.length) toast("success", `Downloaded ${plural(files.length, "workflow file")}`);
  if (skipped.length) {
    toast("warn", `${plural(skipped.length, "job")} had no editor workflow to save`, `Probably queued through the API: ${skipped.join(", ")}`);
  }
}

function defaultName(id, label) {
  return label?.number ? `Job ${label.number}` : `Job ${id.slice(0, 8)}`;
}

function jobName(id, label) {
  return label?.name || defaultName(id, label);
}

function describe(job) {
  const label = store.labels[job.id];
  return {
    title: jobName(job.id, label),
    meta: defaultName(job.id, label),
  };
}

function buildView() {
  const pauseBtn = h("button", { class: "cqm-btn", onclick: () => run(() => setPaused(!store.paused)) });
  const holdBtn = h("button", {
    class: "cqm-btn",
    title: "Queue the current workflow on hold: it waits under On hold and doesn't run until you press its play button",
    onclick: () => run(queueOnHold),
  }, h("i", { class: "pi pi-plus" }), "Queue on hold");
  const saveAllBtn = h("button", { class: "cqm-icon-btn pi pi-save", onclick: () => run(() => saveJobs(null)) });
  const modeBtns = {
    list: h("button", { class: "cqm-seg", onclick: () => setSaveMode("list") }, "On server"),
    files: h("button", { class: "cqm-seg", onclick: () => setSaveMode("files") }, "Workflow files"),
  };
  const status = h("div", { class: "cqm-status" });
  const banner = h("div", { class: "cqm-banner", hidden: true });
  const list = h("div", { class: "cqm-list" });
  const root = h("div", { class: "cqm-root" },
    h("div", { class: "cqm-header" }, h("span", { class: "cqm-title" }, "Queue"), pauseBtn, holdBtn, saveAllBtn),
    h("div", { class: "cqm-mode" }, h("span", { class: "cqm-mode-label" }, "Save to"), modeBtns.list, modeBtns.files),
    status,
    banner,
    list);

  function setSaveMode(mode) {
    store.saveMode = mode;
    try {
      localStorage.setItem(SAVE_MODE_KEY, mode);
    } catch {
      // storage blocked (private window etc.): the toggle still works until the page reloads
    }
    render();
  }

  const saveTitle = () => (store.saveMode === "list" ? "Save to the Saved list" : "Download as a workflow file");

  const drag = { id: null };
  let editingId = null;
  let deferred = false;

  function render() {
    // Rebuilding the list mid-drag or mid-rename would throw away the dragged row or the text box, so wait.
    if (drag.id || editingId) {
      deferred = true;
      return;
    }
    const running = store.jobs.filter(isRunning);
    const pending = store.jobs.filter((job) => !isRunning(job));
    document.body.classList.toggle("cqm-paused", store.paused);

    pauseBtn.className = store.paused ? "cqm-btn cqm-btn--paused" : "cqm-btn";
    pauseBtn.replaceChildren(h("i", { class: store.paused ? "pi pi-play" : "pi pi-pause" }), store.paused ? "Resume" : "Pause");
    saveAllBtn.disabled = store.jobs.length === 0 && store.held.length === 0;
    saveAllBtn.title = store.saveMode === "list" ? "Save every job to the Saved list" : "Download every job as its own workflow file";
    for (const [mode, btn] of Object.entries(modeBtns)) btn.classList.toggle("cqm-seg--active", store.saveMode === mode);

    renderStatus(running.length, pending.length);
    renderBanner();

    const rows = [];
    if (running.length) rows.push(h("div", { class: "cqm-section cqm-section--running" }, "Running"), ...running.map(runningRow));
    if (pending.length) rows.push(h("div", { class: "cqm-section cqm-section--pending" }, "Pending"), ...pending.map((job, i) => pendingRow(job, i)));
    if (store.held.length) rows.push(h("div", { class: "cqm-section cqm-section--held" }, "On hold"), ...store.held.map(heldRow));
    if (!rows.length) rows.push(h("div", { class: "cqm-empty" }, h("i", { class: "pi pi-inbox" }), "The queue is empty"));
    if (store.saved.length) rows.push(h("div", { class: "cqm-section cqm-section--saved" }, "Saved"), ...store.saved.map(savedRow));
    list.replaceChildren(...rows);
    updateProgress();
  }

  function renderStatus(runningCount, pendingCount) {
    const held = store.held.length ? ` · ${store.held.length} on hold` : "";
    let text = `${runningCount} running · ${pendingCount} pending${held}`;
    let warn = true;
    if (!store.connected) text = "Disconnected, waiting for ComfyUI…";
    else if (store.error) text = `Couldn't load the queue: ${store.error}`;
    else if (store.paused) text = `Paused · ${pendingCount} pending${runningCount ? " · the current job will finish" : ""}`;
    else warn = false;
    status.textContent = text;
    status.classList.toggle("cqm-status--warn", warn);
  }

  function renderBanner() {
    const report = store.restore;
    banner.hidden = !report;
    if (!report) {
      banner.replaceChildren();
      return;
    }
    const parts = [];
    if (report.restored) {
      parts.push(h("div", {},
        `Restored ${plural(report.restored, "job")} from the last session`,
        report.interrupted ? " (the one that was running is back at the top)" : "",
        store.paused ? ". The queue is paused." : "."));
    }
    if (report.failed.length) {
      parts.push(
        h("div", {}, `${plural(report.failed.length, "job")} couldn't be restored (saved in data/failed):`),
        h("ul", { class: "cqm-failed" }, ...report.failed.map((failed) =>
          h("li", {}, h("b", {}, jobName(failed.id, failed.label)), `: ${failed.error}`))));
    }
    parts.push(h("div", { class: "cqm-banner-actions" },
      report.restored && store.paused
        ? h("button", { class: "cqm-btn cqm-btn--primary", onclick: () => run(() => setPaused(false)) }, h("i", { class: "pi pi-play" }), "Resume")
        : null,
      h("button", { class: "cqm-btn", onclick: () => run(dismissRestore) }, "Dismiss")));
    banner.replaceChildren(...parts);
  }

  // `save(id, name)` is the request that stores the new name: renameJob for queued jobs, renameSaved for saved ones.
  function titleEl(id, title, label, save) {
    return h("div", { class: "cqm-row-title", title: `${title} (double-click to rename)`, dataset: { id }, ondblclick: () => startRename(id, label, save) }, title);
  }

  function renameBtn(id, label, save) {
    return h("button", { class: "cqm-icon-btn pi pi-pencil", title: "Rename", onclick: () => startRename(id, label, save) });
  }

  function saveBtn(id) {
    return h("button", { class: "cqm-icon-btn pi pi-save", title: saveTitle(), onclick: () => run(() => saveJobs([id])) });
  }

  function startRename(id, label, save) {
    const titleNode = list.querySelector(`.cqm-row-title[data-id="${id}"]`);
    if (!titleNode || editingId) return;
    editingId = id;
    titleNode.closest(".cqm-row").removeAttribute("draggable");
    const original = jobName(id, label);
    const input = h("input", {
      class: "cqm-rename",
      type: "text",
      value: original,
      placeholder: defaultName(id, label),
      maxlength: "100",
    });
    let finished = false;
    const finish = (shouldSave) => {
      if (finished) return;
      finished = true;
      editingId = null;
      const name = input.value.trim();
      deferred = true;
      flushRender();
      if (shouldSave && name !== original) run(() => save(id, name));
    };
    input.addEventListener("keydown", (event) => {
      event.stopPropagation(); // keep ComfyUI's keyboard shortcuts from firing while typing
      if (event.key === "Enter") finish(true);
      else if (event.key === "Escape") finish(false);
    });
    input.addEventListener("blur", () => finish(true));
    titleNode.replaceWith(input);
    input.focus();
    input.select();
  }

  function flushRender() {
    if (deferred && !drag.id && !editingId) {
      deferred = false;
      render();
    }
  }

  function runningRow(job) {
    const { title, meta } = describe(job);
    const label = store.labels[job.id];
    return h("div", { class: "cqm-row cqm-row--running" },
      h("i", { class: "cqm-row-icon pi pi-spin pi-spinner" }),
      h("div", { class: "cqm-row-body" },
        titleEl(job.id, title, label, renameJob),
        h("div", { class: "cqm-row-meta" }, meta),
        h("div", { class: "cqm-progress" }, h("div", { class: "cqm-progress-bar", dataset: { id: job.id } }))),
      renameBtn(job.id, label, renameJob),
      saveBtn(job.id),
      h("button", { class: "cqm-icon-btn pi pi-stop-circle", title: "Stop this job", onclick: () => run(() => cancelJob(job.id)) }));
  }

  function pendingRow(job, index) {
    const { title, meta } = describe(job);
    const label = store.labels[job.id];
    const interrupted = store.restore?.interrupted === job.id;
    return h("div", { class: "cqm-row", draggable: "true", dataset: { id: job.id } },
      h("i", { class: "cqm-row-icon pi pi-bars", title: "Drag to reorder" }),
      h("span", { class: "cqm-pos" }, String(index + 1)),
      h("div", { class: "cqm-row-body" },
        titleEl(job.id, title, label, renameJob),
        h("div", { class: "cqm-row-meta" }, interrupted ? h("span", { class: "cqm-badge" }, "was running") : null, meta)),
      renameBtn(job.id, label, renameJob),
      saveBtn(job.id),
      h("button", { class: "cqm-icon-btn pi pi-times", title: "Remove from queue", onclick: () => run(() => cancelJob(job.id)) }));
  }

  function heldRow(job) {
    const label = store.labels[job.id];
    const title = jobName(job.id, label);
    return h("div", { class: "cqm-row cqm-row--held" },
      h("i", { class: "cqm-row-icon pi pi-pause-circle" }),
      h("div", { class: "cqm-row-body" },
        titleEl(job.id, title, label, renameJob),
        h("div", { class: "cqm-row-meta" }, defaultName(job.id, label))),
      renameBtn(job.id, label, renameJob),
      saveBtn(job.id),
      h("button", { class: "cqm-icon-btn pi pi-play", title: "Release: add to the end of the queue", onclick: () => run(() => releaseHeld(job.id)) }),
      h("button", { class: "cqm-icon-btn pi pi-times", title: "Remove", onclick: () => run(() => removeHeld(job.id)) }));
  }

  function savedRow(entry) {
    const title = jobName(entry.id, entry);
    return h("div", { class: "cqm-row" },
      h("i", { class: "cqm-row-icon pi pi-bookmark" }),
      h("div", { class: "cqm-row-body" },
        titleEl(entry.id, title, entry, renameSaved),
        h("div", { class: "cqm-row-meta" }, defaultName(entry.id, entry))),
      renameBtn(entry.id, entry, renameSaved),
      h("button", { class: "cqm-icon-btn pi pi-replay", title: "Restore: open in a new workflow tab", onclick: () => run(() => restoreSaved(entry.id)) }),
      h("button", { class: "cqm-icon-btn pi pi-download", title: "Download as a workflow file", onclick: () => run(() => downloadSaved(entry.id)) }),
      h("button", { class: "cqm-icon-btn pi pi-play", title: "Add to the queue", onclick: () => run(() => requeueSaved(entry.id)) }),
      h("button", { class: "cqm-icon-btn pi pi-trash", title: "Delete this saved job", onclick: () => run(() => deleteSaved(entry.id, title)) }));
  }

  function updateProgress() {
    for (const bar of list.querySelectorAll(".cqm-progress-bar")) {
      const progress = store.progress[bar.dataset.id];
      bar.style.width = progress?.max ? `${Math.round((100 * progress.value) / progress.max)}%` : "0%";
    }
  }

  function dropTarget(event) {
    const rows = [...list.querySelectorAll(".cqm-row[draggable]")];
    if (!rows.length) return null;
    let row = event.target.closest?.(".cqm-row[draggable]");
    let after;
    if (row) {
      const rect = row.getBoundingClientRect();
      after = event.clientY > rect.top + rect.height / 2;
    } else {
      after = event.clientY > rows[0].getBoundingClientRect().top;
      row = after ? rows[rows.length - 1] : rows[0];
    }
    const next = rows[rows.indexOf(row) + 1];
    return { row, after, before: after ? next?.dataset.id ?? null : row.dataset.id };
  }

  function clearDropMarks() {
    for (const row of list.querySelectorAll(".cqm-drop-before, .cqm-drop-after")) {
      row.classList.remove("cqm-drop-before", "cqm-drop-after");
    }
  }

  function endDrag() {
    clearDropMarks();
    list.querySelector(".cqm-dragging")?.classList.remove("cqm-dragging");
    drag.id = null;
    flushRender();
  }

  list.addEventListener("dragstart", (event) => {
    const row = event.target.closest?.(".cqm-row[draggable]");
    if (!row) return;
    drag.id = row.dataset.id;
    row.classList.add("cqm-dragging");
    event.dataTransfer.effectAllowed = "move";
    event.dataTransfer.setData("text/plain", drag.id);
  });

  list.addEventListener("dragover", (event) => {
    if (!drag.id) return;
    event.preventDefault();
    const target = dropTarget(event);
    clearDropMarks();
    target?.row.classList.add(target.after ? "cqm-drop-after" : "cqm-drop-before");
  });

  list.addEventListener("drop", (event) => {
    if (!drag.id) return;
    event.preventDefault();
    const id = drag.id;
    const target = dropTarget(event);
    endDrag();
    if (target && target.before !== id && target.row.dataset.id !== id) run(() => moveJob(id, target.before));
  });

  list.addEventListener("dragend", endDrag);

  return { root, render, updateProgress };
}

app.registerExtension({
  name: "cherry.QueueManager",

  async setup() {
    document.head.appendChild(h("link", { rel: "stylesheet", href: new URL("./queue_manager.css", import.meta.url).href }));
    view = buildView();
    subscribe();

    app.extensionManager.registerSidebarTab({
      id: TAB_ID,
      icon: "pi pi-list",
      title: "Cherry Queue",
      tooltip: "Cherry Queue: pause, reorder, survives restarts",
      type: "custom",
      render(el) {
        el.style.height = "100%";
        el.appendChild(view.root);
        scheduleRefresh(0);
      },
    });

    refresh();
  },
});
