import random
from quart import Quart, jsonify, abort, request, render_template
import requests
from pradhanStream_v3 import pradhanStreams
import json
import io
import traceback
import PTN
from googleapiclient.discovery import build
import json, os
import pandas as pd
from datetime import datetime, timedelta

app  = Quart(__name__)

pp = pradhanStreams()

api_key = os.getenv('yt_apikey')
youtube = build("youtube",'v3',developerKey=api_key)

MANIFEST = json.load(open('data/MANIFEST.json'))
MANIFEST_TOR = json.load(open('data/MANIFEST_TOR.json'))
MANIFEST_YTrailer = json.load(open('data/MANIFEST_YTrailer.json'))

CATALOG = requests.get("https://raw.githubusercontent.com/piyushpradhan22/imdb-indian/master/data.json").json()
YT_META_URL = "https://raw.githubusercontent.com/piyushpradhan22/imdb-indian/refs/heads/master/yt_meta.json"

# Cache for YT metadata
YT_META_CACHE = {
    'data': None,
    'timestamp': None
}

def get_yt_metadata():
    """Fetch YouTube metadata with 24-hour caching"""
    now = datetime.now()
    
    # Check if cache exists and is still valid (less than 24 hours old)
    if YT_META_CACHE['data'] is not None and YT_META_CACHE['timestamp'] is not None:
        cache_age = now - YT_META_CACHE['timestamp']
        if cache_age < timedelta(hours=24):
            return YT_META_CACHE['data']
    
    # Cache is invalid or doesn't exist, fetch new data
    try:
        yt_meta_data = requests.get(YT_META_URL).json()
        YT_META_CACHE['data'] = yt_meta_data
        YT_META_CACHE['timestamp'] = now
        return yt_meta_data
    except Exception as e:
        # If fetch fails but we have old cache, return it
        if YT_META_CACHE['data'] is not None:
            return YT_META_CACHE['data']
        raise e

def get_ytstream(imdb_id):
    # Fetch the YT metadata from the cache
    try:
        yt_meta_data = get_yt_metadata()
        
        # Normalize the imdb_id format to match the JSON keys (ott format without 'tt')
        normalized_id = imdb_id if not imdb_id.startswith('tt') else 'ott' + imdb_id[2:]
        
        # Check if the imdb_id exists in the metadata
        if normalized_id in yt_meta_data:
            yt_id = yt_meta_data[normalized_id]['yt_id']
            yt_title = yt_meta_data[normalized_id]['yt_title']
            
            return {'streams': [{'name': '🍿YTrailer', 
                                'title': yt_title,
                                'externalUrl': f'https://www.youtube.com/watch?v={yt_id}'}]}
        else:
            # Fallback to the old method if not found in metadata
            imdb_id = imdb_id if imdb_id.startswith('tt') else imdb_id[1:]
            res = requests.get(f"https://cinemeta-live.strem.io/meta/movie/{imdb_id}.json").json()
            query = f"{res['meta']['name']} {res['meta']['releaseInfo']}" if 'releaseInfo' in res['meta'].keys() else res['meta']['name']
            req = youtube.search().list(q = f'{query} Hindi Trailer', part='snippet', type='video')
            res = req.execute()
            
            return {'streams': [{'name': '🍿YTrailer', 
                                'title': res['items'][0]['snippet']['title'],
                                'externalUrl': 'https://www.youtube.com/watch?v=' + res['items'][0]['id']['videoId']}]}
    except Exception as e:
        return {'streams': []}
    #return {'trailers' : [{ "source": res['items'][0]['id']['videoId'], "type": "Trailer" }]}

def get_ytmeta(type, imdb_id):
    # Fetch the YT metadata from the cache
    try:
        yt_meta_data = get_yt_metadata()
        
        # Normalize the imdb_id format
        imdb_id_clean = imdb_id if imdb_id.startswith('tt') else 'tt' + imdb_id[3:]
        normalized_id = 'ott' + imdb_id_clean[2:]
        
        # Get the base metadata from cinemeta
        if type == 'movie':
            meta = requests.get(f"https://cinemeta-live.strem.io/meta/movie/{imdb_id_clean}.json").json()
        elif type == 'series':
            meta = requests.get(f"https://cinemeta-live.strem.io/meta/series/{imdb_id_clean}.json").json()
        
        # Check if the imdb_id exists in the YT metadata
        if normalized_id in yt_meta_data:
            yt_id = yt_meta_data[normalized_id]['yt_id']
            
            # Add the trailer to meta
            meta['meta']['trailers'] = [{"source": yt_id, "type": "Trailer"}]
        else:
            # Fallback to the old YouTube search method
            query = f"{meta['meta']['name']} {meta['meta']['releaseInfo']}" if 'releaseInfo' in meta['meta'].keys() else meta['meta']['name']
            req = youtube.search().list(q = f'{query} Hindi Trailer', part='snippet', type='video')
            res = req.execute()
            
            if res['items']:
                trailers = []
                for i, item in enumerate(res['items'][:2]):  # Get up to 2 trailers
                    trailers.append({"source": item['id']['videoId'], "type": "Trailer"})
                meta['meta']['trailers'] = trailers
        
        return meta
    except Exception as e:
        # Return basic meta without trailers if there's an error
        imdb_id_clean = imdb_id if imdb_id.startswith('tt') else 'tt' + imdb_id[3:]
        meta = requests.get(f"https://cinemeta-live.strem.io/meta/movie/{imdb_id_clean}.json").json()
        return meta

def get_catalog_tor():
    CATALOG_TOR = {}
    CATALOG_TOR["TORRENT"] = [{"id": x['imdb_id'].split(":")[0], "type" : "series" if ":" in x['imdb_id'] else "movie",
                           "poster" : f"https://live.metahub.space/poster/medium/{x['imdb_id'].split(":")[0]}/img"}
                           for _, x in pp.get_catalog_db_pg().iterrows()]
    CATALOG_TOR["TORRENT"] = pd.DataFrame(CATALOG_TOR['TORRENT']).drop_duplicates(subset=['id']).to_dict(orient='records')
    
    return CATALOG_TOR

CATALOG_TOR = get_catalog_tor()

def get_tor_stream(imdb_id):
    streams = []
    for _, x in pp.get_tor_stream_db_pg(imdb_id).iterrows():
        ptn = PTN.parse(x["name"])
        resolution = ptn['resolution'] if 'resolution' in ptn.keys() else ''
        quality = ptn['quality'] if 'quality' in ptn.keys() else ''
        name = f"{resolution} {quality}\nTORR"
        streams.append({"name" : name, "description": f"{x['name']} \n 💾 {float(x['size']) / (1024 ** 3) :10.2f} GB", 
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

def rename(newname):
    def decorator(f):
        f.__name__ = newname
        return f
    return decorator

def dummy_sync():
    return

@app.route("/")
async def hello():
    template = await render_template('helloworld.html')
    return template

@app.route("/configure/")
async def configure():
    if 'https' in request.base_url:
        template = await render_template('Configure.html', stremio_uri = f"{request.base_url.replace('https','stremio').replace('configure.html','').replace('configure/','')}")
    else:
        template = await render_template('Configure.html', stremio_uri = f"{request.base_url.replace('http','stremio').replace('configure.html','').replace('configure/','')}")
    return template

### Catalog Routes
@app.route('/catalog/<type>/<id>.json')
def addon_catalog(type, id):
    global CATALOG
    CATALOG = requests.get("https://raw.githubusercontent.com/piyushpradhan22/imdb-indian/master/data.json").json()
    random.shuffle(CATALOG['Top Rated'])
    metaPreviews = {
        'metas': CATALOG[id][:25]
    }
    return respond_with(metaPreviews)

@app.route('/catalog/<type>/<id>/<skip>.json')
def addon_catalog_skip(type, id, skip):
    metaPreviews = {
        'metas': CATALOG[id][int(skip.split('=')[1]):int(skip.split('=')[1])+25]
    }
    return respond_with(metaPreviews)

@app.route('/tor/catalog/<type>/<id>.json')
def addon_catalog_tor(type, id):
    global CATALOG_TOR
    CATALOG_TOR = get_catalog_tor()
    metaPreviews = {
        'metas': CATALOG_TOR[id][:25]
    }
    return respond_with(metaPreviews)

@app.route('/tor/catalog/<type>/<id>/<skip>.json')
def addon_catalog_skip_tor(type, id, skip):
    metaPreviews = {
        'metas': CATALOG_TOR[id][int(skip.split('=')[1]):int(skip.split('=')[1])+25]
    }
    return respond_with(metaPreviews)

### Manifest ALL Quality and Routes
@app.route(f"/manifest.json")
def addon_manifest_all():
    MANIFEST['name'] = f"🎬PRADHAN"
    return respond_with(MANIFEST)

@app.route(f"/tor/manifest.json")
def addon_manifest_tor():
    MANIFEST_TOR['name'] = f"🎬TOR_HF"
    return respond_with(MANIFEST_TOR)

@app.route(f"/yt/manifest.json")
def addon_manifest_ytrailer():
    MANIFEST_YTrailer['name'] = f"🍿YTrailer"
    return respond_with(MANIFEST_YTrailer)
@app.route('/meta/<type>/<id>.json')
def addon_meta(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    return respond_with(get_ytmeta(type, id))

@app.route(f'/stream/<type>/<id>.json')
async def addon_stream_all(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    
    return respond_with(await pp.pikpak_main(id, type))

@app.route(f'/tor/stream/<type>/<id>.json')
async def addon_stream_tor(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    return respond_with(get_tor_stream(id))

@app.route(f'/yt/stream/<type>/<id>.json')
def addon_stream_yt(type, id):
    if type not in MANIFEST['types'] or ':' in id:
        abort(404)
    return respond_with(get_ytstream(id))

if __name__ == '__main__':
    import uvicorn
    import os
    
    # Check if running in development or production
    debug_mode = os.getenv('DEBUG', 'True').lower() == 'true'
    debug_mode = True # Force production mode
    if debug_mode:
        # Development mode - single process with auto-reload
        print("Running in development mode with auto-reload")
        uvicorn.run("main:app", host="127.0.0.1", port=5000, reload=True)
    else:
        # Production mode - multiple workers for parallel requests
        print("Running in production mode with multiple workers")
        uvicorn.run("main:app", host="0.0.0.0", port=5000, workers=4)