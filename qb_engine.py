import os
import asyncio
import urllib.parse
import re
import requests
import qbittorrentapi
from quart import Response
import config

VIDEO_EXTS = ('.mkv', '.mp4', '.avi', '.ts', '.mov', '.webm', '.m4v')

DEFAULT_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://explodie.org:6969/announce",
    "udp://tracker.dler.com:6969/announce",
    "http://tracker.renfei.net:8080/announce",
    "udp://tracker.nyaa.vc:6969/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://tracker.coppersurfer.tk:6969/announce",
    "udp://exodus.desync.com:6969/announce"
]

class QBWebDAVEngine:
    def __init__(self):
        self.qb_url = config.QBITTORRENT_URL
        self.qb_user = config.QBITTORRENT_USER
        self.qb_pass = config.QBITTORRENT_PASSWORD

        self.webdav_url = config.WEBDAV_URL
        self.webdav_user = config.WEBDAV_USER
        self.webdav_pass = config.WEBDAV_PASSWORD

        self.client = None
        self._init_client()

    def _init_client(self):
        try:
            self.client = qbittorrentapi.Client(
                host=self.qb_url,
                username=self.qb_user,
                password=self.qb_pass,
                REQUESTS_ARGS={'timeout': 10}
            )
            self.client.auth_log_in()
            print(f"[QBWebDAVEngine] Connected to qBittorrent {self.client.app.version} at {self.qb_url}")
        except Exception as e:
            print(f"[QBWebDAVEngine] Failed to connect to qBittorrent: {e}")
            self.client = None

    def get_client(self):
        if self.client is None:
            self._init_client()
        else:
            try:
                _ = self.client.app.version
            except Exception:
                print("[QBWebDAVEngine] Re-authenticating qBittorrent client...")
                self._init_client()
        return self.client

    def build_magnet(self, info_hash: str, sources: list = None) -> str:
        magnet = f"magnet:?xt=urn:btih:{info_hash.lower()}"
        trackers = list(DEFAULT_TRACKERS)
        if sources:
            for s in sources:
                if s.startswith("tracker:"):
                    tr = s[len("tracker:"):]
                    if tr not in trackers:
                        trackers.append(tr)
        for tr in trackers:
            magnet += f"&tr={urllib.parse.quote(tr, safe='')}"
        return magnet

    def build_webdav_stream_urls(self, rel_path: str):
        encoded_parts = [urllib.parse.quote(part) for part in rel_path.replace('\\', '/').split('/')]
        encoded_path = "/".join(encoded_parts)

        clean_url = f"{self.webdav_url}/{encoded_path}"

        parsed = urllib.parse.urlparse(self.webdav_url)
        quoted_user = urllib.parse.quote(self.webdav_user, safe='')
        quoted_pass = urllib.parse.quote(self.webdav_pass, safe='')
        auth_netloc = f"{quoted_user}:{quoted_pass}@{parsed.netloc}"
        direct_url = f"{parsed.scheme}://{auth_netloc}{parsed.path}/{encoded_path}"

        return direct_url, clean_url

    async def add_and_prepare_torrent(self, info_hash: str, file_idx: int = None, filename_hint: str = None, sources: list = None):
        client = self.get_client()
        if not client:
            raise RuntimeError("qBittorrent client not connected")

        info_hash = info_hash.lower()

        existing = client.torrents_info(torrent_hashes=info_hash)
        if not existing:
            magnet = self.build_magnet(info_hash, sources)
            print(f"[QBWebDAVEngine] Adding torrent {info_hash} to qBittorrent...")
            client.torrents_add(
                urls=magnet,
                category="MediaFusion",  # Protected from auto-deletion
                is_sequential_download=True,
                is_first_last_piece_priority=True,
                is_auto_torrent_management=False,
                is_paused=False
            )
        else:
            tor = existing[0]
            if tor.state in ('pausedDL', 'pausedUP', 'stoppedDL', 'stoppedUP'):
                client.torrents_resume(torrent_hashes=info_hash)
            if not getattr(tor, 'seq_dl', False):
                try:
                    client.torrents_toggle_sequential_download(torrent_hashes=info_hash)
                except Exception:
                    pass
            if not getattr(tor, 'f_l_piece_prio', False):
                try:
                    client.torrents_toggle_first_last_piece_priority(torrent_hashes=info_hash)
                except Exception:
                    pass

        # Poll for metadata & file list (up to 40 seconds)
        files = []
        for _ in range(40):
            try:
                files = client.torrents_files(torrent_hash=info_hash)
                if files:
                    break
            except Exception:
                pass
            await asyncio.sleep(1.0)

        if not files:
            raise RuntimeError(f"Timeout waiting for torrent metadata for {info_hash}")

        # Determine target file
        target_file = None
        if file_idx is not None and 0 <= file_idx < len(files):
            target_file = files[file_idx]
        elif filename_hint:
            clean_hint = os.path.basename(filename_hint).lower()
            for f in files:
                if clean_hint in f.name.lower():
                    target_file = f
                    break

        if not target_file:
            video_files = [f for f in files if f.name.lower().endswith(VIDEO_EXTS)]
            if video_files:
                target_file = max(video_files, key=lambda x: x.size)
            else:
                target_file = max(files, key=lambda x: x.size)

        # Set target file priority to maximum (7)
        try:
            client.torrents_file_priority(
                torrent_hash=info_hash,
                file_ids=target_file.index,
                priority=7
            )
        except Exception as e:
            print(f"[QBWebDAVEngine] Priority set error: {e}")

        # Ensure sequential download and first/last piece priority are enabled
        try:
            tors = client.torrents_info(torrent_hashes=info_hash)
            if tors:
                tor = tors[0]
                if not getattr(tor, 'seq_dl', False):
                    client.torrents_toggle_sequential_download(torrent_hashes=info_hash)
                if not getattr(tor, 'f_l_piece_prio', False):
                    client.torrents_toggle_first_last_piece_priority(torrent_hashes=info_hash)
        except Exception:
            pass

        # Wait for initial buffer: verify start piece of target file is downloaded
        start_piece = 0
        if hasattr(target_file, 'piece_range') and target_file.piece_range:
            start_piece = target_file.piece_range[0]

        for _ in range(60):
            try:
                tors = client.torrents_info(torrent_hashes=info_hash)
                if not tors:
                    break
                tor = tors[0]
                if tor.progress == 1.0:
                    break
                piece_states = client.torrents_piece_states(torrent_hash=info_hash)
                if piece_states and len(piece_states) > start_piece:
                    if piece_states[start_piece] == 2:
                        print(f"[QBWebDAVEngine] Buffer ready (piece {start_piece} downloaded) for {target_file.name}")
                        break
            except Exception:
                pass
            await asyncio.sleep(0.5)

        direct_url, clean_url = self.build_webdav_stream_urls(target_file.name)
        return {
            "target_file": target_file,
            "direct_url": direct_url,
            "clean_url": clean_url,
            "rel_path": target_file.name,
            "size": target_file.size
        }

    async def proxy_stream(self, clean_url: str, request_headers: dict):
        req_headers = {}
        for h in ('range', 'Range'):
            if h in request_headers:
                req_headers['Range'] = request_headers[h]
                break

        loop = asyncio.get_event_loop()
        r = await loop.run_in_executor(
            None,
            lambda: requests.get(
                clean_url,
                headers=req_headers,
                auth=(self.webdav_user, self.webdav_pass),
                stream=True,
                timeout=15
            )
        )

        async def chunk_generator():
            try:
                for chunk in r.iter_content(chunk_size=128 * 1024):
                    if chunk:
                        yield chunk
            finally:
                r.close()

        resp_headers = {}
        for h in ['Content-Range', 'Content-Length', 'Content-Type', 'Accept-Ranges', 'ETag']:
            val = r.headers.get(h)
            if val:
                resp_headers[h] = val

        # Ensure inline playback rather than attachment download
        resp_headers['Content-Disposition'] = 'inline'
        resp_headers['Access-Control-Allow-Origin'] = '*'
        resp_headers['Access-Control-Allow-Headers'] = '*'

        return Response(chunk_generator(), status=r.status_code, headers=resp_headers)

    def format_stremio_streams(self, torrents: list, base_url: str) -> list:
        streams = []
        base_url = base_url.rstrip('/')
        for torr in torrents:
            info_hash = torr.get('infoHash')
            if not info_hash:
                continue

            name = torr.get('name', 'Torrentio')
            lines = [l.strip() for l in name.split('\n') if l.strip()]
            quality = lines[-1] if len(lines) > 1 else lines[0]
            stream_name = f"⚡ [qB] {quality}"

            title = torr.get('title', '')
            behavior_hints = torr.get('behaviorHints', {})
            filename = behavior_hints.get('filename', '')
            file_idx = torr.get('fileIdx')

            display_title = title if title else filename
            display_title += "\n⚡ Instant Play via qBittorrent & WebDAV"

            params = []
            if file_idx is not None:
                params.append(f"fileIdx={file_idx}")
            if filename:
                params.append(f"title={urllib.parse.quote(filename)}")
            query_str = f"?{'&'.join(params)}" if params else ""
            playback_url = f"{base_url}/playback/{info_hash}{query_str}"

            stream_entry = {
                "name": stream_name,
                "title": display_title,
                "url": playback_url,
                "behaviorHints": {
                    "bingeGroup": f"qb-{info_hash}",
                }
            }
            if filename:
                stream_entry["behaviorHints"]["filename"] = filename

            streams.append(stream_entry)
        return streams