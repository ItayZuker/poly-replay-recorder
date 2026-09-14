const DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
const DAY_LABEL = { mon: "Mon", tue: "Tue", wed: "Wed", thu: "Thu", fri: "Fri", sat: "Sat", sun: "Sun" };

const marketSelect = document.getElementById("market-select");
const switchBtn = document.getElementById("recording-switch");
const switchLabel = switchBtn.querySelector(".recording-switch-label");
const grid = document.getElementById("week-grid");

let markets = [];
let selectedSeries = "btc-5m";
let switchBusy = false;
let expectedPerHour = 12;
let liveBothSockets = false;

function padHour(hour) {
  return `${String(hour).padStart(2, "0")}:00`;
}

function currentUtcDayHour() {
  const now = new Date();
  const day = DAYS[(now.getUTCDay() + 6) % 7];
  return { day, hour: now.getUTCHours() };
}

function currentChipIndex(expected) {
  const winMin = expected === 4 ? 15 : 5;
  return Math.floor(new Date().getUTCMinutes() / winMin);
}

function buildGrid() {
  grid.replaceChildren();
  const corner = document.createElement("div");
  corner.className = "week-corner";
  corner.textContent = "UTC";
  grid.appendChild(corner);
  for (const day of DAYS) {
    const head = document.createElement("div");
    head.className = "week-day";
    head.dataset.day = day;
    head.textContent = DAY_LABEL[day];
    grid.appendChild(head);
  }
  for (let hour = 0; hour < 24; hour += 1) {
    const label = document.createElement("div");
    label.className = "week-hour";
    label.dataset.hour = String(hour);
    label.textContent = padHour(hour);
    grid.appendChild(label);
    for (const day of DAYS) {
      const slot = document.createElement("div");
      slot.className = "week-slot";
      slot.dataset.day = day;
      slot.dataset.hour = String(hour);
      fillSlotWindows(slot, 12);
      grid.appendChild(slot);
    }
  }
}

function setSwitch(on) {
  switchBtn.classList.toggle("is-on", on);
  switchBtn.setAttribute("aria-pressed", on ? "true" : "false");
  switchLabel.textContent = on ? "On" : "Off";
  grid.classList.toggle("is-recording", on);
  paintNow();
}

function paintNow() {
  const now = currentUtcDayHour();
  grid.querySelectorAll(".is-now").forEach((el) => el.classList.remove("is-now"));
  grid.querySelectorAll(".week-slot-win.is-live").forEach((el) => el.classList.remove("is-live"));
  grid.querySelector(`.week-hour[data-hour="${now.hour}"]`)?.classList.add("is-now");
  grid.querySelector(`.week-day[data-day="${now.day}"]`)?.classList.add("is-now");
  if (!grid.classList.contains("is-recording") || !liveBothSockets) return;
  const slot = grid.querySelector(`.week-slot[data-day="${now.day}"][data-hour="${now.hour}"]`);
  if (!slot) return;
  const chips = slot.querySelectorAll(".week-slot-win");
  const chip = chips[currentChipIndex(chips.length || expectedPerHour)];
  if (!chip) return;
  chip.classList.add("is-live");
  chip.classList.remove("is-missing", "is-recorded");
}

function fillSlotWindows(slot, expected) {
  const chips = slot.querySelectorAll(".week-slot-win");
  if (chips.length === expected) return;
  slot.replaceChildren();
  for (let i = 0; i < expected; i += 1) {
    const chip = document.createElement("div");
    chip.className = "week-slot-win is-missing";
    slot.appendChild(chip);
  }
}

function paintCoverage(payload) {
  const byKey = new Map(
    (payload.slots ?? []).map((s) => [`${s.day}:${s.hour}`, s]),
  );
  expectedPerHour = Number(payload.expectedPerHour) || 12;
  const expected = expectedPerHour;
  for (const day of DAYS) {
    for (let hour = 0; hour < 24; hour += 1) {
      const slot = grid.querySelector(`.week-slot[data-day="${day}"][data-hour="${hour}"]`);
      if (!slot) continue;
      fillSlotWindows(slot, expected);
      const row = byKey.get(`${day}:${hour}`);
      const flags = Array.isArray(row?.windows) ? row.windows : [];
      slot.querySelectorAll(".week-slot-win").forEach((chip, i) => {
        const on = flags[i] === true;
        chip.classList.toggle("is-recorded", on);
        chip.classList.toggle("is-missing", !on);
        chip.classList.remove("is-live");
      });
    }
  }
  liveBothSockets = payload.liveBothSockets === true;
  if (typeof payload.recordingEnabled === "boolean") {
    setSwitch(payload.recordingEnabled);
  }
  paintNow();
}

function fillMarkets(list) {
  markets = Array.isArray(list) ? list : [];
  marketSelect.replaceChildren();
  for (const market of markets) {
    const opt = document.createElement("option");
    opt.value = market._id;
    opt.textContent = market.label || market._id;
    marketSelect.appendChild(opt);
  }
  if (!markets.some((m) => m._id === selectedSeries) && markets[0]) {
    selectedSeries = markets[0]._id;
  }
  marketSelect.value = selectedSeries;
  const selected = markets.find((m) => m._id === selectedSeries);
  setSwitch(Boolean(selected?.recordingEnabled));
}

async function loadMarkets() {
  const res = await fetch("/api/markets");
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || "Failed to load markets");
  fillMarkets(data.markets);
}

async function loadCoverage() {
  const res = await fetch(`/api/week-coverage?series=${encodeURIComponent(selectedSeries)}`);
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || "Failed to load coverage");
  paintCoverage(data);
}

async function toggleRecording() {
  if (switchBusy) return;
  const next = !switchBtn.classList.contains("is-on");
  switchBusy = true;
  switchBtn.disabled = true;
  try {
    const res = await fetch(`/api/markets/${encodeURIComponent(selectedSeries)}/recording`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ recordingEnabled: next }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "Failed to update recording");
    setSwitch(data.recordingEnabled === true);
    const market = markets.find((m) => m._id === selectedSeries);
    if (market) market.recordingEnabled = data.recordingEnabled === true;
  } catch (err) {
    console.error(err);
  } finally {
    switchBusy = false;
    switchBtn.disabled = false;
  }
}

marketSelect.addEventListener("change", () => {
  selectedSeries = marketSelect.value;
  void loadCoverage();
});
switchBtn.addEventListener("click", () => {
  void toggleRecording();
});

buildGrid();
paintNow();

void (async () => {
  try {
    await loadMarkets();
    await loadCoverage();
  } catch (err) {
    console.error(err);
  }
  setInterval(() => {
    void loadCoverage().catch(() => {});
  }, 5_000);
  setInterval(() => {
    void loadMarkets().catch(() => {});
  }, 30_000);
  setInterval(paintNow, 5_000);
})();
