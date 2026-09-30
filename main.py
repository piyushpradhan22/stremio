import config
import random
import os
import json
import io
import traceback
import urllib.parse

from quart import Quart, jsonify, abort, request, render_template, redirect
import requests
import pandas as pd
import PTN

from pradhanStream_v3 import pradhanStreams
from qb_engine import QBWebDAVEngine

app = Quart(__name__)

pp = pradhanStreams()
qb_engine = QBWebDAVEngine()

MANIFEST = json.load(open('data/MANIFEST.json'))
MANIFEST_TOR = json.load(open('data/MANIFEST_TOR.json'))
MANIFEST_QB = json.load(open('data/MANIFEST_QB.json'))

CATALOG = requests.get("https://raw.githubusercontent.com/piyushpradhan22/imdb-indian/master/data.json").json()

def get_base_url():
    addon_url = config.ADDON_URL or os.getenv('ADDON_URL')
    if addon_url:
        return addon_url.rstrip('/')
    proto = request.headers.get('X-Forwarded-Proto', request.scheme)
    host = request.headers.get('X-Forwarded-Host', request.host)
    return f"{proto}://{host}"

def get_meta(type, imdb_id):
    try:
        imdb_id_clean = imdb_id if imdb_id.startswith('tt') else 'tt' + imdb_id[3:]
        if type == 'movie':
            return requests.get(f"https://cinemeta-live.strem.io/meta/movie/{imdb_id_clean}.json").json()
        elif type == 'series':
            return requests.get(f"https://cinemeta-live.strem.io/meta/series/{imdb_id_clean}.json").json()
        return {}
    except Exception:
        return {}

def get_catalog_tor():
    try:
        CATALOG_TOR = {}
        CATALOG_TOR["TORRENT"] = [{"id": x['imdb_id'].split(":")[0], "type" : "series" if ":" in x['imdb_id'] else "movie",
                               "poster" : f"https://live.metahub.space/poster/medium/{x['imdb_id'].split(':')[0]}/img",
                               "name" : x['name']}
                               for _, x in pp.get_catalog_db_pg().iterrows()]
        CATALOG_TOR["TORRENT"] = pd.DataFrame(CATALOG_TOR['TORRENT']).drop_duplicates(subset=['id']).to_dict(orient='records')
        return CATALOG_TOR
    except Exception as e:
        print(f"Error fetching catalog tor: {e}")
        return {"TORRENT": []}

CATALOG_TOR = get_catalog_tor()

def get_tor_stream(imdb_id):
    streams = []
    for _, x in pp.get_tor_stream_db_pg(imdb_id).iterrows():
        ptn = PTN.parse(x["name"])
        resolution = ptn.get('resolution', '')
        quality = ptn.get('quality', '')
        name = f"{resolution} {quality}\nTORR"
        streams.append({"name" : name, "description": f"{x['name']} \n \U0001f4be {float(x['size']) / (1024 ** 3) :10.2f} GB", 
                        "url" : x["url"]})

    return {"streams" : streams}

def respond_with(data):
    resp = jsonify(data)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Headers'] = '*'
    return resp

def get_exception_traceback_str(exc: Exception) -> str:
    file = io.StringIO()
    traceback.print_exception(exc, file=file)
    return file.getvalue().rstrip()

@app.route("/")
async def hello():
    template = await render_template('helloworld.html')
    return template

@app.route("/configure/")
@app.route("/configure")
async def configure():
    base_url = get_base_url().rstrip('/') + '/'
    stremio_uri = base_url.replace('https://', 'stremio://').replace('http://', 'stremio://')
    template = await render_template('Configure.html', stremio_uri=stremio_uri, http_uri=base_url)
    return template

# ==========================================
# 1. EXISTING ADDON: PRADHAN (PikPak) - UNTOUCHED
# ==========================================
@app.route("/manifest.json")
def addon_manifest_all():
    MANIFEST['name'] = "\U0001f3acPRADHAN"
    return respond_with(MANIFEST)

@app.route('/catalog/<type>/<id>.json')
def addon_catalog(type, id):
    global CATALOG
    CATALOG = requests.get("https://raw.githubusercontent.com/piyushpradhan22/imdb-indian/master/data.json").json()
    random.shuffle(CATALOG.get('Top Rated', []))
    metaPreviews = {
        'metas': CATALOG.get(id, [])[:25]
    }
    return respond_with(metaPreviews)

@app.route('/catalog/<type>/<id>/<skip>.json')
def addon_catalog_skip(type, id, skip):
    skip_val = int(skip.split('=')[1]) if '=' in skip else int(skip)
    metaPreviews = {
        'metas': CATALOG.get(id, [])[skip_val:skip_val+25]
    }
    return respond_with(metaPreviews)

@app.route('/meta/<type>/<id>.json')
def addon_meta(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    return respond_with(get_meta(type, id))

@app.route('/stream/<type>/<id>.json')
async def addon_stream_all(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    return respond_with(await pp.pikpak_main(id, type))

# ==========================================
# 2. EXISTING ADDON: TOR_HF (Postgres HF) - UNTOUCHED
# ==========================================
@app.route("/tor/manifest.json")
def addon_manifest_tor():
    MANIFEST_TOR['name'] = "\U0001f4beTOR_HF"
    return respond_with(MANIFEST_TOR)

@app.route('/tor/catalog/<type>/<id>.json')
def addon_catalog_tor(type, id):
    global CATALOG_TOR
    CATALOG_TOR = get_catalog_tor()
    metaPreviews = {
        'metas': CATALOG_TOR.get(id, [])[:25]
    }
    return respond_with(metaPreviews)

@app.route('/tor/catalog/<type>/<id>/<skip>.json')
def addon_catalog_skip_tor(type, id, skip):
    skip_val = int(skip.split('=')[1]) if '=' in skip else int(skip)
    metaPreviews = {
        'metas': CATALOG_TOR.get(id, [])[skip_val:skip_val+25]
    }
    return respond_with(metaPreviews)

@app.route('/tor/stream/<type>/<id>.json')
async def addon_stream_tor(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    return respond_with(get_tor_stream(id))

# ==========================================
# 3. NEW ADDON: QB (Pure Streams Provider - No Catalogs)
# ==========================================
@app.route("/qb/manifest.json")
def addon_manifest_qb():
    MANIFEST_QB['name'] = "⚡QB"
    return respond_with(MANIFEST_QB)

@app.route('/qb/stream/<type>/<id>.json')
async def addon_stream_qb(type, id):
    if type not in MANIFEST_QB['types']:
        abort(404)

    torrents = pp.get_torrents(id, type) if type == 'movie' else pp.get_series_torrents(id)
    if not torrents:
        return respond_with({"streams": []})

    base_url = get_base_url()
    streams = qb_engine.format_stremio_streams(torrents, base_url)
    return respond_with({"streams": streams})

# Playback & Range Proxy Handlers
@app.route('/playback/<info_hash>')
@app.route('/qb/playback/<info_hash>')
async def addon_playback(info_hash):
    file_idx = request.args.get('fileIdx', type=int)
    title = request.args.get('title', '')
    use_proxy = request.args.get('proxy', '0') == '1'

    try:
        prep = await qb_engine.add_and_prepare_torrent(
            info_hash,
            file_idx=file_idx,
            filename_hint=title
        )
        if use_proxy:
            return await qb_engine.proxy_stream(prep['clean_url'], dict(request.headers))

        resp = redirect(prep['direct_url'], code=302)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Headers'] = '*'
        return resp
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/proxy/<info_hash>')
@app.route('/qb/proxy/<info_hash>')
async def addon_proxy(info_hash):
    file_idx = request.args.get('fileIdx', type=int)
    title = request.args.get('title', '')
    try:
        prep = await qb_engine.add_and_prepare_torrent(
            info_hash,
            file_idx=file_idx,
            filename_hint=title
        )
        return await qb_engine.proxy_stream(prep['clean_url'], dict(request.headers))
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    import uvicorn
    debug_mode = os.getenv('DEBUG', 'True').lower() == 'true'
    if debug_mode:
        print("Running in development mode with auto-reload")
        uvicorn.run("main:app", host="127.0.0.1", port=5000, reload=True)
    else:
        print("Running in production mode with multiple workers")
        uvicorn.run("main:app", host="0.0.0.0", port=5000, workers=4)