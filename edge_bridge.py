import json
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qsl, urlencode, urlsplit

import websocket


CDP = "http://127.0.0.1:9222"
HOST = "0.0.0.0"
PORT = 8765
TIMEOUT = 30

# Для первой версии обрабатываем запросы последовательно.
browser_lock = threading.Lock()


def json_bytes(data):
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def browser_json_request(api_url):
    # Создаём отдельную вкладку Edge.
    request = urllib.request.Request(
        CDP + "/json/new?about:blank",
        method="PUT",
    )

    with urllib.request.urlopen(request, timeout=5) as response:
        target = json.load(response)

    ws = None

    try:
        ws = websocket.create_connection(
            target["webSocketDebuggerUrl"],
            timeout=TIMEOUT,
            suppress_origin=True,
        )

        next_id = 0

        def command(method, params=None):
            nonlocal next_id
            next_id += 1
            command_id = next_id

            message = {"id": command_id, "method": method}
            if params is not None:
                message["params"] = params

            ws.send(json.dumps(message))

            while True:
                reply = json.loads(ws.recv())

                if reply.get("id") != command_id:
                    continue

                if "error" in reply:
                    raise RuntimeError(
                        f"Ошибка Edge CDP: {reply['error']}"
                    )

                return reply.get("result", {})

        command("Page.enable")
        command("Page.navigate", {"url": api_url})

        # Ждём окончания загрузки страницы.
        while True:
            event = json.loads(ws.recv())
            if event.get("method") == "Page.loadEventFired":
                break

        result = command(
            "Runtime.evaluate",
            {
                "expression": "document.body.innerText",
                "returnByValue": True,
            },
        )

        body = result.get("result", {}).get("value")
        if not body:
            raise RuntimeError("Edge вернул пустое содержимое страницы")

        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "Ответ Edge не является JSON. Начало ответа: "
                + body[:300]
            ) from exc

    finally:
        if ws is not None:
            ws.close()

        # Закрываем созданную вкладку, не затрагивая остальные.
        try:
            urllib.request.urlopen(
                CDP + "/json/close/" + target["id"],
                timeout=3,
            ).close()
        except Exception:
            pass


class BridgeHandler(BaseHTTPRequestHandler):
    def send_json(self, status, data):
        body = json_bytes(data)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlsplit(self.path)

        if parsed.path == "/health":
            try:
                with urllib.request.urlopen(
                    CDP + "/json/version", timeout=3
                ) as response:
                    browser = json.load(response).get("Browser", "Edge")

                self.send_json(200, {
                    "status": "ok",
                    "browser": browser,
                })
            except Exception as exc:
                self.send_json(503, {
                    "error": "Edge недоступен",
                    "details": str(exc),
                })
            return

        # Не разрешаем использовать посредник для произвольных сайтов.
        if parsed.path != "/v1/forecast":
            self.send_json(404, {
                "error": "Поддерживается только /v1/forecast и /health"
            })
            return

        params = parse_qsl(parsed.query, keep_blank_values=True)

        if not params:
            self.send_json(400, {
                "error": "Передай параметры прогноза в URL"
            })
            return

        api_url = (
            "https://api.open-meteo.com/v1/forecast?"
            + urlencode(params)
        )

        try:
            with browser_lock:
                data = browser_json_request(api_url)

            self.send_json(200, data)

        except Exception as exc:
            print(f"Ошибка запроса: {exc}", flush=True)
            self.send_json(502, {
                "error": "Не удалось получить JSON через Edge",
                "details": str(exc),
            })

    def log_message(self, format_string, *args):
        print(
            f"[HTTP] {self.address_string()} - "
            + (format_string % args),
            flush=True,
        )


if __name__ == "__main__":
    # Проверяем связь с Edge до запуска сервера.
    with urllib.request.urlopen(CDP + "/json/version", timeout=5) as r:
        version = json.load(r).get("Browser", "Edge")

    server = ThreadingHTTPServer((HOST, PORT), BridgeHandler)

    print(f"Edge: {version}", flush=True)
    print(f"Посредник запущен на порту {PORT}", flush=True)
    print("Проверка: http://127.0.0.1:8765/health", flush=True)
    print("Для остановки нажми Ctrl+C.", flush=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nОстанавливаем посредник...", flush=True)
    finally:
        server.server_close()
