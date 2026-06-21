import xmlrpc.client
from typing import Any
from urllib.parse import urlparse, urlunparse
from xml.parsers.expat import ExpatError

import requests

from app.providers.downloaders.base import BaseDownloaderProvider


class RTorrentProvider(BaseDownloaderProvider):
    """rTorrent / ruTorrent provider speaking XML-RPC over HTTP(S).

    rTorrent exposes XML-RPC through an SCGI socket that is typically fronted by
    nginx (``scgi_pass``) at a URL path, e.g. ``https://host/<user>``. MediaSync
    POSTs an XML-RPC body to that endpoint with optional HTTP Basic auth, exactly
    like Sonarr/Radarr's built-in rTorrent client.

    Credentials follow the same convention as the other providers: pass
    ``username:password`` in the API Key field, embed them in the URL, or supply
    the dedicated username/password fields (the API packs them into ``api_key``).
    """

    TIMEOUT_SECONDS = 10
    # Seedbox front-ends sometimes use self-signed certificates; rTorrent download
    # clients conventionally don't verify. Kept as an attribute so it's easy to flip.
    VERIFY_SSL = True

    # d.multicall2 commands, in the order they are unpacked in normalize_download_item.
    QUEUE_COMMANDS = [
        "d.name=",
        "d.hash=",
        "d.size_bytes=",
        "d.completed_bytes=",
        "d.left_bytes=",
        "d.down.rate=",
        "d.up.rate=",
        "d.state=",
        "d.complete=",
        "d.hashing=",
        "d.message=",
        "d.is_active=",
        "d.ratio=",
        "d.directory=",
        "d.custom1=",
        "d.peers_connected=",
        "d.peers_complete=",
    ]

    def __init__(self, server_url: str, api_key: str):
        super().__init__(server_url, api_key)
        self.username = ""
        self.password = ""

        parsed_url = urlparse(self.server_url)

        if parsed_url.username or parsed_url.password:
            self.username = parsed_url.username or ""
            self.password = parsed_url.password or ""
            host = parsed_url.hostname or ""
            port = f":{parsed_url.port}" if parsed_url.port else ""
            self.server_url = urlunparse(
                (parsed_url.scheme or "http", f"{host}{port}", parsed_url.path, "", "", "")
            ).rstrip("/")

        if self.api_key:
            if ":" in self.api_key:
                self.username, self.password = self.api_key.split(":", 1)
            elif not self.password:
                self.password = self.api_key

    def _auth(self) -> tuple[str, str] | None:
        if self.username or self.password:
            return (self.username, self.password)

        return None

    def _call(self, method: str, *params: Any) -> dict[str, Any]:
        if not self.server_url:
            return {"success": False, "message": "rTorrent URL is required."}

        body = xmlrpc.client.dumps(tuple(params), method)

        try:
            response = requests.post(
                self.server_url,
                data=body.encode("utf-8"),
                headers={"Content-Type": "text/xml"},
                auth=self._auth(),
                timeout=self.TIMEOUT_SECONDS,
                verify=self.VERIFY_SSL,
            )

            if response.status_code in (401, 403):
                return {
                    "success": False,
                    "message": "rTorrent authentication failed. Check username/password.",
                    "status_code": response.status_code,
                }

            response.raise_for_status()

            parsed, _ = xmlrpc.client.loads(response.text)
            return {"success": True, "data": parsed[0] if parsed else None}

        except xmlrpc.client.Fault as fault:
            return {"success": False, "message": f"rTorrent XML-RPC fault: {fault.faultString}"}
        except requests.exceptions.SSLError:
            return {
                "success": False,
                "message": "rTorrent SSL verification failed. Check the certificate or use http://.",
            }
        except requests.exceptions.ConnectionError:
            return {"success": False, "message": "Could not connect to rTorrent. Check the server URL."}
        except requests.exceptions.Timeout:
            return {"success": False, "message": "rTorrent connection timed out."}
        except (ExpatError, xmlrpc.client.ResponseError):
            return {"success": False, "message": "rTorrent returned an invalid XML-RPC response."}
        except requests.RequestException as error:
            return {"success": False, "message": f"rTorrent request failed: {error}"}

    def test_connection(self) -> dict[str, Any]:
        result = self._call("system.client_version")

        if not result.get("success"):
            return result

        version = str(result.get("data") or "Unknown").strip()

        return {
            "success": True,
            "message": f"Connected to rTorrent {version}.",
            "version": version,
            "status": "Connected",
            "connected": 1,
        }

    def get_status(self) -> dict[str, Any]:
        result = self._call("throttle.global_down.rate")

        if not result.get("success"):
            return result

        rate = self._to_int(result.get("data"))

        return {
            "success": True,
            "message": "rTorrent status read successfully.",
            "data": {"dl_info_speed": rate},
            "speed": self._format_bytes_per_second(rate),
        }

    def get_queue(self) -> dict[str, Any]:
        result = self._call("d.multicall2", "", "main", *self.QUEUE_COMMANDS)

        if not result.get("success"):
            return result

        rows = result.get("data") or []

        if not isinstance(rows, list):
            rows = []

        downloads = [
            self.normalize_download_item(row)
            for row in rows
            if isinstance(row, (list, tuple))
        ]

        active_downloads = [
            item for item in downloads
            if item.get("status") in {
                "downloading",
                "queued",
                "checking",
                "verifying",
                "stalled",
                "paused",
                "seeding",
            }
        ]

        total_speed = sum(self._to_int(item.get("_dlspeed")) for item in downloads)

        return {
            "success": True,
            "message": "rTorrent queue read successfully.",
            "downloads": downloads,
            "active_count": len(active_downloads),
            "total_count": len(downloads),
            "speed": self._format_bytes_per_second(total_speed),
            "timeleft": self._queue_eta(downloads),
            "size": self._queue_size(downloads),
            "raw": rows,
        }

    def get_history(self, limit: int = 80) -> dict[str, Any]:
        result = self.get_queue()

        if not result.get("success"):
            return result

        history = []

        for item in result.get("downloads") or []:
            status = str(item.get("status") or "").strip().lower()

            if status == "completed":
                final_state = "completed"
            elif status == "failed":
                final_state = "failed"
            else:
                final_state = "unknown"

            history_item = dict(item)
            history_item["final_state"] = final_state
            history.append(history_item)

        return {
            "success": True,
            "message": "rTorrent history read successfully.",
            "history": history[:limit],
        }

    def normalize_download_item(self, item: Any, queue: dict[str, Any] | None = None) -> dict[str, Any]:
        row = list(item) if isinstance(item, (list, tuple)) else []
        row += [None] * (len(self.QUEUE_COMMANDS) - len(row))

        (name, info_hash, size_bytes, completed_bytes, left_bytes, down_rate, up_rate,
         state, complete, hashing, message, is_active, ratio, directory, custom1,
         peers_connected, peers_complete) = row[:17]

        size = self._to_int(size_bytes)
        done = self._to_int(completed_bytes)
        left = self._to_int(left_bytes)
        dlspeed = self._to_int(down_rate)
        percent = round((done / size * 100), 1) if size > 0 else 0.0
        status = self._normalize_status(
            state=self._to_int(state),
            complete=self._to_int(complete),
            hashing=self._to_int(hashing),
            is_active=self._to_int(is_active),
            dlspeed=dlspeed,
            message=str(message or ""),
        )
        eta_seconds = int(left / dlspeed) if dlspeed > 0 and left > 0 else -1

        return {
            "id": str(info_hash or name or ""),
            "hash": str(info_hash or ""),
            "name": str(name or "Unknown download"),
            "filename": str(name or "Unknown download"),
            "category": str(custom1 or ""),
            "status": status,
            "status_code": str(state),
            "percent": percent,
            "size": self._format_bytes(size),
            "remaining": self._format_bytes(left),
            "speed": self._format_bytes_per_second(dlspeed),
            "upload_speed": self._format_bytes_per_second(self._to_int(up_rate)),
            "eta": self._format_eta(eta_seconds),
            "peers": self._to_int(peers_connected),
            "seeders": self._to_int(peers_complete),
            "leechers": max(0, self._to_int(peers_connected) - self._to_int(peers_complete)),
            "ratio": round(self._to_int(ratio) / 1000.0, 3),
            "download_dir": str(directory or ""),
            "save_path": str(directory or ""),
            "content_path": str(directory or ""),
            "error": 1 if status == "failed" else 0,
            "errorString": str(message or "") if status == "failed" else "",
            "fail_message": str(message or "") if status == "failed" else "",
            "_dlspeed": dlspeed,
            "_size": size,
            "_eta": eta_seconds,
            "raw": row,
        }

    _ERROR_MARKERS = ("unregistered", "not registered", "failed", "denied", "no such file")

    def _normalize_status(self, state: int, complete: int, hashing: int, is_active: int,
                          dlspeed: int, message: str) -> str:
        if hashing and hashing > 0:
            return "checking"

        lowered = message.strip().lower()
        if lowered and any(marker in lowered for marker in self._ERROR_MARKERS):
            return "failed"

        if state == 0:
            return "paused"

        if complete == 1:
            return "seeding"

        if dlspeed > 0:
            return "downloading"

        if is_active == 1:
            return "stalled"

        return "queued"

    def _queue_eta(self, downloads: list[dict[str, Any]]) -> str:
        eta_values = [
            self._to_int(download.get("_eta"), default=-1)
            for download in downloads
            if self._to_int(download.get("_eta"), default=-1) >= 0
            and download.get("status") in {"downloading", "queued", "stalled"}
        ]

        if not eta_values:
            return ""

        return self._format_eta(min(eta_values))

    def _queue_size(self, downloads: list[dict[str, Any]]) -> str:
        return self._format_bytes(sum(self._to_int(d.get("_size")) for d in downloads))

    def _format_eta(self, seconds: int) -> str:
        if seconds < 0 or seconds >= 8640000:
            return ""

        if seconds < 60:
            return f"{seconds}s"

        minutes, remaining_seconds = divmod(seconds, 60)

        if minutes < 60:
            return f"{minutes}m {remaining_seconds}s"

        hours, remaining_minutes = divmod(minutes, 60)

        if hours < 24:
            return f"{hours}h {remaining_minutes}m"

        days, remaining_hours = divmod(hours, 24)
        return f"{days}d {remaining_hours}h"

    def _format_bytes_per_second(self, value: int) -> str:
        return f"{self._format_bytes(value)}/s"

    def _format_bytes(self, value: int) -> str:
        size = max(0.0, self._to_float(value))
        units = ["B", "KB", "MB", "GB", "TB", "PB"]

        for unit in units:
            if size < 1024 or unit == units[-1]:
                if unit == "B":
                    return f"{int(size)} {unit}"

                return f"{size:.1f} {unit}"

            size = size / 1024

        return f"{size:.1f} PB"

    def _to_float(self, value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _to_int(self, value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
