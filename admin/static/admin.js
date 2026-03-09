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
});
