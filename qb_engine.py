import os
import time
import asyncio
import urllib.parse
import httpx
import requests
import qbittorrentapi
from quart import Response
import config

TORRSERVER_URL = "http://172.17.0.1:8090"
VIDEO_EXTS = ('.mkv', '.mp4', '.avi', '.ts', '.mov', '.webm', '.m4v')

DEFAULT_TRACKERS = [
    'udp://tracker.opentrackr.org:1337/announce',
    'udp://open.stealth.si:80/announce',
    'udp://tracker.torrent.eu.org:451/announce',
    'udp://explodie.org:6969/announce',
    'udp://tracker.dler.com:6969/announce',
    'http://tracker.renfei.net:8080/announce',
    'udp://tracker.nyaa.vc:6969/announce',
    'udp://open.demonii.com:1337/announce',
    'udp://tracker.coppersurfer.tk:6969/announce',
    'udp://exodus.desync.com:6969/announce',
]


def log(msg):
    print(f'[QB-HYBRID] {msg}', flush=True)


class QBWebDAVEngine:
    def __init__(self):
        self.qb_url = config.QBITTORRENT_URL
        self.qb_user = config.QBITTORRENT_USER
        self.qb_pass = config.QBITTORRENT_PASSWORD
        self.webdav_url = config.WEBDAV_URL.rstrip('/')
        self.webdav_user = config.WEBDAV_USER
        self.webdav_pass = config.WEBDAV_PASSWORD
        self.ts_url = TORRSERVER_URL
        self.client = None
        self._prepared_cache = {}
        self._preparing_tasks = {}
        self.stream_dir = '/tmp/active_streams'
        os.makedirs(self.stream_dir, exist_ok=True)
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
            log(f'Connected to qBittorrent {self.client.app.version} at {self.qb_url}')
        except Exception as e:
            log(f'ERROR: Failed to connect to qBittorrent: {e}')
            self.client = None

    def get_client(self):
        if self.client is None:
            self._init_client()
        else:
            try:
                _ = self.client.app.version
            except Exception:
                log('Re-authenticating qBittorrent client...')
                self._init_client()
        return self.client

    def mark_active(self, info_hash: str):
        try:
            fpath = os.path.join(self.stream_dir, f'{info_hash.lower()}.ts')
            with open(fpath, 'w') as f:
                f.write(str(time.time()))
        except Exception:
            pass

    def is_active(self, info_hash: str, max_idle: float = 180.0) -> bool:
        try:
            fpath = os.path.join(self.stream_dir, f'{info_hash.lower()}.ts')
            if os.path.exists(fpath):
                with open(fpath, 'r') as f:
                    ts = float(f.read().strip())
                return (time.time() - ts) < max_idle
        except Exception:
            pass
        return False

    def get_idle_seconds(self, info_hash: str) -> float:
        try:
            fpath = os.path.join(self.stream_dir, f'{info_hash.lower()}.ts')
            if os.path.exists(fpath):
                with open(fpath, 'r') as f:
                    ts = float(f.read().strip())
                return round(time.time() - ts, 1)
        except Exception:
            pass
        return 999999.0

    def build_magnet(self, info_hash: str, sources: list = None) -> str:
        magnet = f'magnet:?xt=urn:btih:{info_hash.lower()}'
        trackers = list(DEFAULT_TRACKERS)
        if sources:
            for s in sources:
                tr = s[len('tracker:'):] if s.startswith('tracker:') else s
                if tr and tr not in trackers:
                    trackers.append(tr)
        for tr in trackers:
            magnet += f'&tr={urllib.parse.quote(tr, safe="")}'
        return magnet

    def build_webdav_stream_urls(self, rel_path: str):
        encoded_parts = [urllib.parse.quote(part, safe='') for part in rel_path.replace('\\', '/').split('/')]
        encoded_path = '/'.join(encoded_parts)
        clean_url = f'{self.webdav_url}/{encoded_path}'
        parsed = urllib.parse.urlparse(self.webdav_url)
        quoted_user = urllib.parse.quote(self.webdav_user, safe='')
        quoted_pass = urllib.parse.quote(self.webdav_pass, safe='')
        auth_netloc = f'{quoted_user}:{quoted_pass}@{parsed.netloc}'
        direct_url = f'{parsed.scheme}://{auth_netloc}{parsed.path}/{encoded_path}'
        return direct_url, clean_url

    async def add_and_prepare_torrent(self, info_hash: str, file_idx: int = None, filename_hint: str = None, sources: list = None):
        info_hash = info_hash.lower()
        cache_key = (info_hash, file_idx)

        cached = self._prepared_cache.get(cache_key)
        if cached:
            # Re-verify qBittorrent is active
            client = self.get_client()
            if client:
                try:
                    client.torrents_resume(torrent_hashes=info_hash)
                except Exception:
                    pass
            return cached

        if cache_key in self._preparing_tasks:
            return await self._preparing_tasks[cache_key]

        task = asyncio.create_task(self._do_add_and_prepare(info_hash, file_idx, filename_hint, sources))
        self._preparing_tasks[cache_key] = task
        try:
            res = await task
            self._prepared_cache[cache_key] = res
            return res
        finally:
            self._preparing_tasks.pop(cache_key, None)

    async def _do_add_and_prepare(self, info_hash: str, file_idx: int = None, filename_hint: str = None, sources: list = None):
        client = self.get_client()
        if not client:
            raise RuntimeError('qBittorrent client not connected')

        log(f'=== HYBRID PREPARE START | hash={info_hash} | fileIdx={file_idx} | hint={filename_hint}')
        magnet = self.build_magnet(info_hash, sources)

        # 1. Add to qBittorrent (Master Download Manager: saves to /home/ubuntu/Downloads, visible in Web UI)
        existing = client.torrents_info(torrent_hashes=info_hash)
        if not existing:
            log(f'STEP1: Adding torrent to qBittorrent...')
            client.torrents_add(
                urls=magnet,
                category='MediaFusion',
                is_auto_torrent_management=False,
                is_sequential_download=True,
                is_first_last_piece_priority=True,
                add_to_top_of_queue=True,
                is_paused=False
            )
            log(f'STEP1: Added to qBittorrent with category=MediaFusion (monitoring enabled)')
        else:
            tor = existing[0]
            log(f'STEP1: Torrent already in qBittorrent | state={tor.state} | progress={tor.progress:.2%} | dl={tor.downloaded/1024/1024:.1f}MB')
            if getattr(tor, 'category', '') != 'MediaFusion':
                client.torrents_set_category(torrent_hashes=info_hash, category='MediaFusion')
            client.torrents_resume(torrent_hashes=info_hash)

        # 2. Register with TorrServer (Seek Booster: RAM-only on-demand piece delivery)
        async with httpx.AsyncClient(timeout=10.0) as http_c:
            try:
                await http_c.post(f'{self.ts_url}/torrents', json={
                    'action': 'add',
                    'link': magnet,
                    'save_to_db': True
                })
                log('STEP2: Registered with TorrServer seek booster')
            except Exception as e:
                log(f'STEP2: Notice TorrServer register: {e}')

        # 3. Wait for metadata & file list from qBittorrent
        log('STEP3: Waiting for metadata (up to 30s)...')
        files = []
        for i in range(30):
            try:
                files = client.torrents_files(torrent_hash=info_hash)
                if files:
                    log(f'STEP3: Metadata received! Got {len(files)} file(s) in {i+1}s')
                    break
            except Exception:
                pass
            await asyncio.sleep(1.0)

        if not files:
            raise RuntimeError(f'Timeout waiting for metadata for torrent {info_hash}')

        # 4. Determine target file
        target_file = None
        if filename_hint:
            clean_hint = os.path.basename(filename_hint).strip().lower()
            hint_stem = os.path.splitext(clean_hint)[0]
            for f in files:
                f_base = os.path.basename(f.name).lower()
                if clean_hint == f_base or hint_stem in f_base or f_base in clean_hint:
                    target_file = f
                    log(f'STEP4: Matched file by hint -> [{f.index}] {f.name}')
                    break

        if not target_file and file_idx is not None:
            for f in files:
                if getattr(f, 'index', None) == file_idx:
                    target_file = f
                    log(f'STEP4: Matched file by index -> [{f.index}] {f.name}')
                    break
            if not target_file and 0 <= file_idx < len(files):
                target_file = files[file_idx]
                log(f'STEP4: Matched file by position -> [{target_file.index}] {target_file.name}')

        if not target_file:
            video_files = [f for f in files if f.name.lower().endswith(VIDEO_EXTS)]
            target_file = max(video_files if video_files else files, key=lambda x: x.size)
            log(f'STEP4: Fallback largest video file -> [{target_file.index}] {target_file.name}')

        # Single video torrent optimization for qBittorrent
        video_files = [f for f in files if f.name.lower().endswith(VIDEO_EXTS)]
        if len(video_files) > 1:
            try:
                for vf in video_files:
                    p = 7 if vf.index == target_file.index else 0
                    client.torrents_file_priority(torrent_hash=info_hash, file_ids=vf.index, priority=p)
            except Exception as e:
                log(f'STEP4: Priority config: {e}')

        # Ensure qBittorrent sequential download is active
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

        props = client.torrents_properties(torrent_hash=info_hash)
        piece_size = props.piece_size
        start_piece, end_piece = target_file.piece_range
        file_start_byte = sum(f.size for f in files if f.index < target_file.index)

        # 5. Resolve TorrServer target file ID
        ts_file_id = None
        async with httpx.AsyncClient(timeout=10.0) as http_c:
            for _ in range(10):
                try:
                    resp = await http_c.post(f'{self.ts_url}/torrents', json={'action': 'get', 'hash': info_hash})
                    if resp.status_code == 200:
                        ts_files = resp.json().get('file_stats', [])
                        if ts_files:
                            target_base = os.path.basename(target_file.name).lower()
                            for tf in ts_files:
                                if target_base in tf['path'].lower():
                                    ts_file_id = tf['id']
                                    break
                            if ts_file_id is None:
                                ts_file_id = ts_files[0]['id']
                            break
                except Exception:
                    pass
                await asyncio.sleep(0.5)

        direct_url, clean_url = self.build_webdav_stream_urls(target_file.name)
        ts_stream_url = f'{self.ts_url}/stream?link={info_hash}&index={ts_file_id or 1}&play'

        result = {
            'info_hash': info_hash,
            'target_file': target_file,
            'direct_url': direct_url,
            'clean_url': clean_url,
            'rel_path': target_file.name,
            'size': target_file.size,
            'piece_size': piece_size,
            'file_start_byte': file_start_byte,
            'start_piece': start_piece,
            'end_piece': end_piece,
            'ts_stream_url': ts_stream_url
        }

        self.mark_active(info_hash)
        log(f'=== HYBRID PREPARE COMPLETE | {info_hash} ({target_file.name})')
        return result

    async def proxy_stream(self, prep_or_url, request_headers: dict, method: str = 'GET'):
        if isinstance(prep_or_url, dict):
            prep = prep_or_url
        else:
            clean_url = prep_or_url
            prep = next((v for v in self._prepared_cache.values() if v.get('clean_url') == clean_url), None)
            if not prep:
                return await self._proxy_direct_webdav(clean_url, request_headers, method)

        info_hash = prep['info_hash']
        self.mark_active(info_hash)

        clean_url = prep['clean_url']
        file_size = prep['size']
        piece_size = prep['piece_size']
        file_start_byte = prep.get('file_start_byte', 0)
        ts_stream_url = prep.get('ts_stream_url')

        range_val = request_headers.get('range') or request_headers.get('Range')
        log(f'PROXY: {method} {prep["rel_path"].split("/")[-1][:40]} | Range={range_val}')

        # Robust RFC-compliant Range parsing
        start_byte = 0
        end_byte = file_size - 1
        if range_val and range_val.startswith('bytes='):
            range_spec = range_val[len('bytes='):].strip()
            if range_spec.startswith('-'):
                # Suffix byte range: e.g. bytes=-65536
                try:
                    suffix = abs(int(range_spec.lstrip('-').split('-')[0]))
                    start_byte = max(0, file_size - suffix)
                except Exception:
                    start_byte = 0
            elif '-' in range_spec:
                parts = range_spec.split('-', 1)
                try:
                    start_byte = int(parts[0]) if parts[0] else 0
                except Exception:
                    start_byte = 0
                try:
                    # Clean trailing characters like -1 if malformed
                    clean_end = parts[1].split('-')[0].strip() if parts[1] else ''
                    end_byte = min(int(clean_end), file_size - 1) if clean_end else file_size - 1
                except Exception:
                    end_byte = file_size - 1

        content_length = end_byte - start_byte + 1
        status = 206 if range_val else 200

        # MIME type
        fname = clean_url.split('/')[-1].split('?')[0].lower()
        if fname.endswith('.mkv'):
            content_type = 'video/x-matroska'
        elif fname.endswith('.mp4'):
            content_type = 'video/mp4'
        elif fname.endswith('.avi'):
            content_type = 'video/x-msvideo'
        elif fname.endswith('.ts'):
            content_type = 'video/mp2t'
        elif fname.endswith('.webm'):
            content_type = 'video/webm'
        else:
            content_type = 'video/mp4'

        resp_headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            'Accept-Ranges': 'bytes',
            'Content-Disposition': 'inline',
            'Content-Type': content_type,
            'Content-Length': str(content_length)
        }
        if range_val:
            resp_headers['Content-Range'] = f'bytes {start_byte}-{end_byte}/{file_size}'

        if method.upper() == 'HEAD':
            return Response(b'', status=status, headers=resp_headers)

        # Check if the requested start piece is already downloaded on disk by qBittorrent
        torrent_byte = file_start_byte + start_byte
        p_idx = int(torrent_byte // piece_size)
        start_piece = prep.get('start_piece', 0)
        end_piece = prep.get('end_piece', 0)

        serve_mode = os.getenv('SERVE_MODE', 'qb').lower()
        is_downloaded_on_disk = False

        if serve_mode != 'torr':
            try:
                qb = self.get_client()
                if qb:
                    states = qb.torrents_piece_states(torrent_hash=info_hash)
                    if states and p_idx < len(states):
                        if states[p_idx] == 2:
                            is_downloaded_on_disk = True
                        else:
                            # Optimization: Wait for qB if it's already downloading, prioritized, or near sequential playback
                            is_active_or_prio = (states[p_idx] == 1) or (abs(p_idx - start_piece) <= 2) or (abs(p_idx - end_piece) <= 2)
                            is_sequential_next = False
                            if not is_active_or_prio:
                                for i in range(max(0, p_idx - 3), p_idx):
                                    if states[i] in (1, 2):
                                        is_sequential_next = True
                                        break

                            should_wait = (serve_mode == 'qb') or is_active_or_prio or is_sequential_next

                            if should_wait:
                                log(f"PROXY: Waiting for piece {p_idx} (state={states[p_idx]}) in qB...")
                                max_wait = 15 if serve_mode == 'hybrid' else 45
                                for _ in range(max_wait):
                                    await asyncio.sleep(1.0)
                                    states = qb.torrents_piece_states(torrent_hash=info_hash)
                                    if states and states[p_idx] == 2:
                                        is_downloaded_on_disk = True
                                        log(f"PROXY: Piece {p_idx} is ready in qB!")
                                        break
            except Exception as e:
                log(f"PROXY: piece state check error: {e}")

        # Target WebDAV URL for local disk read
        webdav_target = clean_url
        if self.webdav_url in clean_url:
            webdav_target = clean_url.replace(self.webdav_url, 'http://172.17.0.1:5244/dav')

        async def unified_hybrid_generator():
            current_offset = start_byte
            client = httpx.AsyncClient(
                auth=(self.webdav_user, self.webdav_pass),
                timeout=httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=None)
            )

            try:
                while current_offset <= end_byte:
                    p_idx = int((file_start_byte + current_offset) // piece_size)

                    is_on_disk = False
                    contiguous_disk_pieces = 0

                    if serve_mode in ('hybrid', 'qb'):
                        try:
                            qb = self.get_client()
                            if qb:
                                states = qb.torrents_piece_states(torrent_hash=info_hash)
                                if states and p_idx < len(states) and states[p_idx] == 2:
                                    is_on_disk = True
                                    # Count how many contiguous pieces are ready on disk
                                    while p_idx + contiguous_disk_pieces < len(states) and states[p_idx + contiguous_disk_pieces] == 2:
                                        contiguous_disk_pieces += 1
                        except Exception:
                            pass

                    target_source = 'booster' if serve_mode == 'torr' else 'disk'
                    if serve_mode == 'hybrid':
                        target_source = 'disk' if is_on_disk else 'booster'

                    if target_source == 'disk':
                        if not is_on_disk and serve_mode == 'qb':
                            log(f'PROXY [WAIT]: Mode=qb, piece {p_idx} not ready. Waiting 2s...')
                            await asyncio.sleep(2.0)
                            continue

                        # Stream up to the last contiguous piece available on disk to avoid reading zeroes
                        safe_end = ((p_idx + contiguous_disk_pieces) * piece_size) - file_start_byte - 1
                    elif serve_mode == 'hybrid':
                        # Stop at the next piece boundary so we can check if qB has caught up
                        safe_end = ((p_idx + 1) * piece_size) - file_start_byte - 1
                    else:
                        # Pure torr mode, stream to end of file
                        safe_end = end_byte

                    safe_end_byte = min(end_byte, safe_end)
                    if safe_end_byte < current_offset:
                        safe_end_byte = end_byte

                    url = webdav_target if target_source == 'disk' else ts_stream_url
                    sub_headers = {'Range': f'bytes={current_offset}-{safe_end_byte}'}

                    log(f'PROXY [STREAM]: offset {current_offset}/{end_byte} | src={target_source} | chunk={safe_end_byte-current_offset+1}b')

                    req = client.build_request('GET', url, headers=sub_headers)
                    upstream = await client.send(req, stream=True)
                    try:
                        if upstream.status_code not in (200, 206):
                            log(f'PROXY [ERROR]: {target_source} returned {upstream.status_code}. Retrying in 1s...')
                            await asyncio.sleep(1.0)
                            continue

                        async for chunk in upstream.aiter_bytes(chunk_size=128 * 1024):
                            yield chunk
                            self.mark_active(info_hash)
                            current_offset += len(chunk)
                    finally:
                        await upstream.aclose()

            except asyncio.CancelledError:
                pass
            except Exception as e:
                log(f'PROXY [ERROR]: Unified stream error: {e}')
            finally:
                await client.aclose()

        return Response(unified_hybrid_generator(), status=status, headers=resp_headers)

    async def _proxy_direct_webdav(self, clean_url: str, request_headers: dict, method: str = 'GET'):
        range_val = request_headers.get('range') or request_headers.get('Range')
        headers = {}
        if range_val:
            headers['Range'] = range_val

        target_url = clean_url
        if self.webdav_url in clean_url:
            target_url = clean_url.replace(self.webdav_url, 'http://172.17.0.1:5244/dav')

        client = httpx.AsyncClient(
            auth=(self.webdav_user, self.webdav_pass),
            timeout=httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=None)
        )
        req = client.build_request(method, target_url, headers=headers)
        upstream = await client.send(req, stream=True)

        resp_headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            'Accept-Ranges': upstream.headers.get('accept-ranges', 'bytes'),
            'Content-Disposition': 'inline',
        }
        for h in ('content-type', 'content-length', 'content-range'):
            if h in upstream.headers:
                resp_headers[h.title()] = upstream.headers[h]

        status = upstream.status_code
        if method.upper() == 'HEAD':
            await upstream.aclose()
            await client.aclose()
            return Response(b'', status=status, headers=resp_headers)

        async def chunk_gen():
            try:
                async for chunk in upstream.aiter_bytes(chunk_size=256 * 1024):
                    yield chunk
            except asyncio.CancelledError:
                pass
            finally:
                await upstream.aclose()
                await client.aclose()

        return Response(chunk_gen(), status=status, headers=resp_headers)

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
            stream_name = f'[QB] {quality}'

            title = torr.get('title', '')
            behavior_hints = torr.get('behaviorHints', {})
            filename = behavior_hints.get('filename', '')
            if not filename and title:
                first_line = title.split('\n')[0].strip()
                if any(ext in VIDEO_EXTS for ext in [os.path.splitext(first_line.lower())[1]]) or '.' in first_line:
                    filename = first_line
            file_idx = torr.get('fileIdx')

            display_title = title if title else filename

            params = []
            if file_idx is not None:
                params.append(f'fileIdx={file_idx}')
            if filename:
                params.append(f'title={urllib.parse.quote(filename)}')
            
            sources = torr.get('sources', [])
            for s in sources:
                params.append(f'tr={urllib.parse.quote(s)}')

            query_str = f'?{"&".join(params)}' if params else ''
            playback_url = f'{base_url}/playback/{info_hash}{query_str}'

            stream_entry = {
                'name': stream_name,
                'title': display_title,
                'url': playback_url,
                'behaviorHints': {'bingeGroup': f'qb-{info_hash}'}
            }
            if filename:
                stream_entry['behaviorHints']['filename'] = filename

            streams.append(stream_entry)
        return streams
