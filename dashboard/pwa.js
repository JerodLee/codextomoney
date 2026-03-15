(() => {
  const installBtn = document.getElementById("installAppBtn");
  let deferredPrompt = null;

  const showInstall = (show) => {
    if (!installBtn) return;
    installBtn.hidden = !show;
  };

  window.addEventListener("beforeinstallprompt", (event) => {
    event.preventDefault();
    deferredPrompt = event;
    showInstall(true);
  });

  if (installBtn) {
    installBtn.addEventListener("click", async () => {
      if (deferredPrompt) {
        deferredPrompt.prompt();
        try {
          await deferredPrompt.userChoice;
        } catch (_) {
          // no-op
        }
        deferredPrompt = null;
        showInstall(false);
        return;
      }
      alert("브라우저 메뉴에서 '홈 화면에 추가'를 선택하면 앱처럼 실행할 수 있습니다.");
    });
  }

  window.addEventListener("appinstalled", () => {
    deferredPrompt = null;
    showInstall(false);
  });

  if ("serviceWorker" in navigator) {
    window.addEventListener("load", () => {
      navigator.serviceWorker.register("./service-worker.js?v=20260316-2").catch(() => undefined);
    });
  }
})();

