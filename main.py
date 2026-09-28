import random
from quart import Quart, jsonify, abort, request, render_template
import requests
from pradhanStream_v3 import pradhanStreams
import json
import io
import traceback
import PTN
import os
import pandas as pd

app = Quart(__name__)

pp = pradhanStreams()

MANIFEST = json.load(open('data/MANIFEST.json'))
MANIFEST_TOR = json.load(open('data/MANIFEST_TOR.json'))

CATALOG = requests.get("https://raw.githubusercontent.com/piyushpradhan22/imdb-indian/master/data.json").json()

def get_meta(type, imdb_id):
    try:
        imdb_id_clean = imdb_id if imdb_id.startswith('tt') else 'tt' + imdb_id[3:]
        if type == 'movie':
            return requests.get(f"https://cinemeta-live.strem.io/meta/movie/{imdb_id_clean}.json").json()
        elif type == 'series':
            return requests.get(f"https://cinemeta-live.strem.io/meta/series/{imdb_id_clean}.json").json()
        return {}
    except Exception as e:
        return {}

def get_catalog_tor():
    CATALOG_TOR = {}
    CATALOG_TOR["TORRENT"] = [{"id": x['imdb_id'].split(":")[0], "type" : "series" if ":" in x['imdb_id'] else "movie",
                           "poster" : f"https://live.metahub.space/poster/medium/{x['imdb_id'].split(':')[0]}/img",
                           "name" : x['name']}
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
@app.route("/manifest.json")
def addon_manifest_all():
    MANIFEST['name'] = "\U0001f3acPRADHAN"
    return respond_with(MANIFEST)

@app.route("/tor/manifest.json")
def addon_manifest_tor():
    MANIFEST_TOR['name'] = "\U0001f4beTOR_HF"
    return respond_with(MANIFEST_TOR)

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

@app.route('/tor/stream/<type>/<id>.json')
async def addon_stream_tor(type, id):
    if type not in MANIFEST['types']:
        abort(404)
    return respond_with(get_tor_stream(id))

if __name__ == '__main__':
    import uvicorn
    
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
