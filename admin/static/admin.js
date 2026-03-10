// Auto-scroll log output to bottom
document.addEventListener("DOMContentLoaded", function () {
  var logEl = document.getElementById("log-output");
  if (logEl) {
    logEl.scrollTop = logEl.scrollHeight;
  }

  // Confirm dialogs for destructive actions
  document.querySelectorAll("form[data-confirm]").forEach(function (form) {
    form.addEventListener("submit", function (e) {
      if (!confirm(form.dataset.confirm)) {
        e.preventDefault();
      }
    });
  });

  // Convert UTC timestamps to local time (Eastern)
  var TZ = "America/New_York";
  var opts = {
    timeZone: TZ,
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: true,
  };

  document.querySelectorAll(".utc-time").forEach(function (el) {
    var raw = el.getAttribute("data-utc");
    if (!raw) return;

    // Try parsing the timestamp
    // systemd format: "Tue 2026-03-10 02:25:00 UTC"
    // ISO format: "2026-03-10T02:25:00+00:00"
    var cleaned = raw
      .replace(/^[A-Z][a-z]{2}\s+/, "") // strip leading day name
      .replace(" UTC", "+00:00")        // convert UTC suffix to offset
      .replace(" ", "T");               // space to T for ISO format

    var d = new Date(cleaned);
    if (isNaN(d.getTime())) {
      // Try raw parse as fallback
      d = new Date(raw);
    }
    if (isNaN(d.getTime())) return;

    el.textContent = d.toLocaleString("en-US", opts) + " ET";
  });
});
