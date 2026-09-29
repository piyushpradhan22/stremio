from concurrent import futures
import random
from pikpakapi import PikPakApi
import asyncio
import time
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
import os

class pradhanStreams:
    def __init__(self) -> None:

        self.pikpak_password = os.getenv('password')
        self.postgres_engine = create_engine(os.getenv('postgres_url'), poolclass=NullPool)
        self.downloadWaitS = 10
        self.torr_qualities = ['4k', '4k HDR', '1080p', '1080p DV', '1080p DV | HDR', '1080p DV | HDR10+', '1080p HDR', '720p']
        self.device_id = None
        self.login_time = 0
        self.pikpak_clients = None

        self.webshare_api_key = os.getenv('webshare_api_key') or os.getenv('WEBSHARE_API_KEY')
        self.use_proxy = os.getenv('use_proxy', 'False').lower() in ('true', '1', 't')
        self.proxies = []
        if self.use_proxy:
            self.init_proxies()

    def init_proxies(self):
        if not self.webshare_api_key:
            print("Warning: use_proxy is True but webshare_api_key is not set in environment.")
            return
        try:
            url = "https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=25"
            headers = {"Authorization": f"Token {self.webshare_api_key}"}
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                results = data.get("results", [])
                self.proxies = [
                    f"http://{p['username']}:{p['password']}@{p['proxy_address']}:{p['port']}"
                    for p in results if p.get("valid", True)
                ]
                print(f"Initialized {len(self.proxies)} Webshare proxies.")
            else:
                print(f"Failed to fetch Webshare proxies: {r.status_code} - {r.text}")
        except Exception as e:
            print(f"Error fetching Webshare proxies: {e}")
    
    def get_torrents(self,imdb_id, type):
        url = f"https://torrentio.strem.fun/language=hindi|qualityfilter=480p,other,scr,cam,unknown|sizefilter=6GB/stream/{type}/{imdb_id}.json"
        #url = f"https://torrentio.strem.fun/stream/{type}/{imdb_id}.json"
        #print(url, self.get_response(url, waitTimeGet=2).text)
        resp = self.get_response(url, waitTimeGet=6)
        if not resp:
            return []
        try:
            torrs = resp.json().get('streams', [])
        except Exception:
            return []
        collection_keywords = ['complete', 'collection', 'pack','moviesup']
        torrents = []
        torr_lower = []
        for x in torrs:
            col_exist = False
            for w in collection_keywords:
                if w in x['title'].lower():
                    col_exist = True
            if 'GB' in x['title'].split("💾 ")[1].split(" ⚙️")[0]:
                if float(x['title'].split("💾 ")[1].split(" ⚙️")[0].split(" GB")[0]) < 6:
                    x['size'] = float(x['title'].split("💾 ")[1].split(" ⚙️")[0].split(" ")[0]) * 1024
                    if not col_exist:
                        torrents.append(x)
                    else:
                        torr_lower.append(x)
            elif 'MB' in x['title'].split("💾 ")[1].split(" ⚙️")[0]:
                x['size'] = float(x['title'].split("💾 ")[1].split(" ⚙️")[0].split(" ")[0])
                if not col_exist:
                    torrents.append(x)
                else:
                    torr_lower.append(x)
        torrents.extend(torr_lower)

        return torrents
    
    def get_series_torrents(self, imdb_id, max_episode_no=20):
        url = "https://torrentio.strem.fun/language=hindi|qualityfilter=480p,other,scr,cam,unknown|sizefilter=6GB/stream/series/{}.json"
        episode = int(imdb_id.split(":")[2])
        imdb_season = imdb_id.split(":")[0] + ":" + imdb_id.split(":")[1]

        urls = [url.format(imdb_season+':'+str(i)) for i in range(1, max_episode_no+1)]
        
        torr_flattened = []
        
        with futures.ThreadPoolExecutor(max_workers=4) as executor:
            torrs = list(executor.map(self.get_response, urls))
        
        for x in torrs:
            if x is not None and x.status_code == 200:
                try:
                    torr_flattened.extend(x.json().get('streams', []))
                except Exception:
                    pass
        torr_flattened = [j for sub in torr_flattened for j in sub]
        
        if len(torr_flattened) == 0:
            return []
        
        torrents = []

        for x in torr_flattened:
            if 'GB' in x['title'].split("💾 ")[1].split(" ⚙️")[0]:
                if float(x['title'].split("💾 ")[1].split(" ⚙️")[0].split(" GB")[0]) < 6:
                    x['size'] = float(x['title'].split("💾 ")[1].split(" ⚙️")[0].split(" ")[0]) * 1024
                    torrents.append(x)
            elif 'MB' in x['title'].split("💾 ")[1].split(" ⚙️")[0]:
                x['size'] = float(x['title'].split("💾 ")[1].split(" ⚙️")[0].split(" ")[0])
                torrents.append(x)
        df = pd.DataFrame(torrents)
        df = df[['infoHash', 'size']].groupby(['infoHash']).sum()
        
        torrents = []
        
        episode_torrs = []
        if episode - 1 < len(torrs) and torrs[episode-1] is not None and torrs[episode-1].status_code == 200:
            try:
                episode_torrs = torrs[episode-1].json().get('streams', [])
            except Exception:
                episode_torrs = []

        for x in episode_torrs:
            if len(df[df.index == x['infoHash']]) == 0:
                continue
            size = df[df.index == x['infoHash']].values[0][0]
            if size < 6100:
                x['size'] = df[df.index == x['infoHash']].values[0][0]
                torrents.append(x)    
        return torrents
    
    def get_size_of_torrents(self, torrents):
        url = "https://whatslink.info/api/v1/link?url=magnet:?xt=urn:btih:{}"
        urls = [url.format(x['infoHash']) for x in torrents]
        with futures.ThreadPoolExecutor(max_workers=4) as executor:
            sizes_resp = list(executor.map(self.get_response, urls))
        sizes = []
        for x in sizes_resp:
            try:
                if x is not None and x.status_code == 200:
                    sizes.append(int(x.json().get('size', 0) / (1024**2)))
                else:
                    sizes.append(0)
            except Exception:
                sizes.append(0)
    
        for i in range(len(sizes)):
            if sizes[i] == 0:
                if 'GB' in torrents[i]['title'].split("💾 ")[1].split(" ⚙️")[0]:
                    sizes[i] = int(float(torrents[i]['title'].split("💾 ")[1].split(" ⚙️")[0].split(" ")[0])) * 1024
                elif 'MB' in torrents[i]['title'].split("💾 ")[1].split(" ⚙️")[0]:
                    sizes[i] = int(float(torrents[i]['title'].split("💾 ")[1].split(" ⚙️")[0].split(" ")[0]))
                    

        return sizes
    
    def get_headers(self):
        ip = f"{str(random.randint(0,200))}.{str(random.randint(0,200))}.{str(random.randint(0,200))}.{str(random.randint(0,200))}"
        headers = {'Connection' : 'keep-alive',
                'User-Agent' : 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) QtWebEngine/5.15.2 Chrome/83.0.4103.122 Safari/537.36 StremioShell/4.4.168',
                'Accept': '*/*',
                'Origin' : 'https://app.strem.io' ,
                'Sec-Fetch-Site' : 'cross-site',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest' : 'empty',
                'Accept-Encoding' : 'gzip, deflate, br',
                'X-Forwarded-For' : ip,
                'X-Forwarded-Host' : ip,
                'X-Client-IP' : ip,
                'X-Remote-IP' : ip,
                'X-Remote-Addr' : ip,
                'X-Host' : ip,}
        
        return headers
    
    def get_response(self, url, wait_time=5, waitTimeGet=6):
        response = None
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*'
        }

        proxy_pool = []
        if self.use_proxy and self.proxies:
            proxy_pool = self.proxies.copy()
            random.shuffle(proxy_pool)

        attempts = max(wait_time, len(proxy_pool)) if proxy_pool else wait_time

        for i in range(attempts):
            try:
                proxies_dict = None
                if self.use_proxy and proxy_pool:
                    p = proxy_pool[i % len(proxy_pool)]
                    proxies_dict = {'http': p, 'https': p}

                response = requests.get(url, headers=headers, proxies=proxies_dict, timeout=waitTimeGet)
                if response.status_code == 200:
                    return response
            except Exception:
                pass
        return response
    
    def get_response_w_headers(self,url, wait_time=10, waitTimeGet=2):
        response = None
        for i in range(wait_time):
            try:
                response = requests.get(url,headers=self.get_headers(), timeout=waitTimeGet)
                break
            except Exception as e:
                pass
        return response
    
    def get_next_torr(self, torrents):
        while len(torrents) > 0:
            selecetd_torrents = []
            torrents_c = torrents.copy()
            for torr in torrents_c:
                if torr['name'] not in [x['name'] for x in selecetd_torrents]:
                    selecetd_torrents.append(torr)
                    torrents.remove(torr)
            yield selecetd_torrents
    
    def get_next_emails(self,n):
        df = pd.read_sql(f"SELECT * from selected_email limit {str(n)}", con=self.postgres_engine)
        return df.email.tolist()
    
    def update_email_used(self, email):
        Session = sessionmaker(bind=self.postgres_engine)
        with Session() as session:
            truncate_query = text(f"update email set used = True where email = '{email}'")
            session.execute(truncate_query)
            session.commit()
    
    def delete_email(self, email):
        Session = sessionmaker(bind=self.postgres_engine)
        with Session() as session:
            query = text(f"delete from email where email = '{email}';")
            session.execute(query)
            session.commit()
    
    async def initialize_pikpak(self, email, login_emails_counter = 0):
        pikpak_client = PikPakApi(email, self.pikpak_password)
        try:
            await self.get_with_timeout(pikpak_client.login())
        except:
            if login_emails_counter > 5:
                raise Exception(f"Unable to login via : {email} after Retry")   
            print(f"Unable to login via : {email}")
            self.delete_email(email)
            await self.initialize_pikpak(self.get_next_emails(1)[0], login_emails_counter+1)

        self.device_id = pikpak_client.device_id
        self.encoded_token = pikpak_client.encoded_token
        #quota = await self.get_quota_info_v1(pikpak_client)
        #free_space = (int(quota['quota']['limit']) - int(quota['quota']['usage']))/ (1024**2)
        #pikpak_client.free_space = free_space
        pikpak_client.login_time = time.time()
        self.login_time = time.time()
        return pikpak_client
    
    async def get_new_pikpak_client(self, n):
        emails =self.get_next_emails(n)
        pikpak_clients = await asyncio.gather(*[self.initialize_pikpak(email) for email in emails])
        return pikpak_clients
    
    async def get_with_await(self,corr,awaitWaitS : int =5):
        sleep_t = 0.2
        for i in range(awaitWaitS* int(awaitWaitS//sleep_t)):
            try:
                res = await corr
                return res
            except:
                time.sleep(.2)
    async def get_with_timeout(self,corr,awaitWaitS : int =5):
        try:
            return await asyncio.wait_for(corr, awaitWaitS)
        except:
            pass
    async def get_Mytask_Status(self, tasks : list):
        lst = {'tasks' : []}

        l_tasks = await asyncio.gather(*[self.get_with_await(task['pikpak_client'].offline_list()) for task in tasks])
        lst['tasks'].extend([task['tasks'][0] for task in l_tasks if len(task['tasks']) !=0])

        #for task in tasks:
        #    lst['tasks'].extend((await self.get_with_await(task['pikpak_client'].offline_list()))['tasks'])

        status = []
        try:
            task_ids = [task['task']['id'] for task in tasks]
        except Exception as e:
            print(task_ids)
            raise Exception(e)
            

        for id in task_ids:
            currS = [x for x in lst['tasks'] if x['id']== id]
            status.extend(currS)
        completed_task_ids = set(task_ids) - set([x['id'] for x in status if len(x)!=0])
        completed_file_ids = set([x['task']['file_id'] for x in tasks if x['task']['id'] in completed_task_ids])
        
        return completed_task_ids, completed_file_ids, status
    
    async def wait_for_task_complete(self, tasks, waitS=10):
        ### Waiting for download to complete fully
        for i in range(waitS):
            completed_task, _, _ = await self.get_Mytask_Status(tasks)
            if len(completed_task)==len(tasks):
                return completed_task, True
            else:
                time.sleep(1)
        else:
            return completed_task, False
    
    async def get_pikpak_files(self,id, pikpak_client : PikPakApi):
        try:
            files = (await pikpak_client.file_list(parent_id=id))['files']
        except:
            print('File_ID : ', id, "\n\n", self.valid_tasks)
        return files
    
    async def get_download_url(self, id, pikpak_client : PikPakApi):
        return await pikpak_client.get_download_url(id)
    
    async def get_stream(self, parent_id, file_name, pikpak_client : PikPakApi):
        files = await self.get_pikpak_files(parent_id, pikpak_client)
        if files==None:
            print("no file list", files)
            return {}
        if len(files) ==0:
            file_info = await self.get_download_url(parent_id, pikpak_client)
            return file_info
        else:
            temp_ids = files.copy()
            vid_files = []
            for i in range(200):
                if len(temp_ids) == 0:
                    break
                for idx in temp_ids:
                    if idx['name'] == file_name:
                        return await self.get_download_url(idx['id'], pikpak_client)
                else:
                    files = await self.get_pikpak_files(temp_ids[0]['id'], pikpak_client)
                    
                    if len(files) ==0:
                        file_info = await self.get_download_url(temp_ids[0]['id'], pikpak_client)
                        if temp_ids[0]['name'] == file_name:
                            return  file_info
                        else:
                            vid_files.append(file_info)
                    else :
                        temp_ids.extend(files)
                    temp_ids.remove(temp_ids[0])
            return max(vid_files, key=lambda x : int(x['size']))
    
    def write_dataframe_to_db(self, df):
        df = df.astype(str)
        df.to_sql(name='pikpak_v2', con=self.postgres_engine, if_exists='append', index=False)
    
    def write_torrent_to_DB_pg(self, torrents, imdb_id, imdb_type):
        w_torrents = []
    
        for tor in torrents:
            x = {}
            x['imdb_id'] = imdb_id
            x['type'] = imdb_type
            x['quality'] = tor['name'].split("\n")[1]
            x['title'] = tor['title']
            x['infoHash'] = tor['infoHash']
            x['size'] = tor['size']
            try:
                x['filename'] = tor['behaviorHints']['filename']
            except:
                x['filename'] = ''
            w_torrents.append(x)

        df = pd.DataFrame(w_torrents)
        try:
            df.to_sql(name='pikpak_torrents', con=self.postgres_engine, if_exists='append', index=False)
        except:
            pass

    async def post_responce_cleanup(self):

        ### filter selected parameters to write to db
        self.w_streams = []
        for task in self.valid_tasks:
            tsk = {}

            tsk["type"] =  task["type"]
            tsk["imdb_id"] = task["imdb_id"]
            tsk['quality'] =  task['quality']
            tsk['title'] =  task['title']
            tsk['filename'] =  task['behaviorHints']['filename']
            tsk['file_id'] =  task['id']
            tsk['size'] =  task['size']
            tsk['file_extension'] =  task['file_extension']
            tsk['infoHash'] = task['infoHash']
            tsk['base_file_id'] = task['task']['file_id']
            
            pikpak_login_vars = vars(task['pikpak_client'])
            tsk['username'] = pikpak_login_vars['username']
            tsk['encoded_token'] = pikpak_login_vars['encoded_token']
            tsk['access_token'] = pikpak_login_vars['access_token']
            tsk['refresh_token'] = pikpak_login_vars['refresh_token']
            tsk['user_id'] = pikpak_login_vars['user_id']
            tsk['device_id'] = pikpak_login_vars['device_id']
            tsk['login_time'] = time.time()
            tsk['time'] = time.time()

            self.w_streams.append(tsk)
        try:
            self.write_dataframe_to_db(pd.DataFrame(self.w_streams))
        except:
            pass
    
    ###### Check Existing downloads
    ### Get imdb from db
    def get_catalog_db_pg(self):
        df = pd.read_sql("Select distinct imdb_id, name, time from hftor order by time desc", con=self.postgres_engine)
        return df
    
    def get_tor_stream_db_pg(self, imdb_id):
        df = pd.read_sql(f"Select * from hftor where imdb_id = '{imdb_id}'", con=self.postgres_engine)
        return df
    
    ### Get imdb from db
    def get_data_from_db_pg(self, imdb_id):
        df = pd.read_sql(f"Select * from pikpak_v2 where imdb_id = '{imdb_id}'", con=self.postgres_engine)
        return df
    
    ### Get imdb from db
    def get_torrent_data_from_db_pg(self, imdb_id):
        df = pd.read_sql(f"Select * from pikpak_torrents where imdb_id = '{imdb_id}'", con=self.postgres_engine)
        return df

    def get_series_from_db_pg(self, imdb_id):
        imdb_id_copy = imdb_id.split(":")[0]
        df = pd.read_sql(f"Select * from pikpak_v2 where imdb_id like '{imdb_id_copy}%%'", con=self.postgres_engine)
        return df
    
    def clear_record_with_imdb(self, imdb_id):
        Session = sessionmaker(bind=self.postgres_engine)
        with Session() as session:
            query = text(f"delete from pikpak_v2 where imdb_id = '{imdb_id}'")
            session.execute(query)
            session.commit()
    
    def clear_torrent_with_imdb(self, imdb_id):
        Session = sessionmaker(bind=self.postgres_engine)
        with Session() as session:
            query = text(f"delete from pikpak_torrents where imdb_id = '{imdb_id}'")
            session.execute(query)
            session.commit()
    
    def get_pikpak_client(self, d):
        pikpak = PikPakApi(d['username'], self.pikpak_password)
        pikpak.encoded_token = d['encoded_token']
        pikpak.access_token = d['access_token']
        pikpak.refresh_token = d['refresh_token']
        pikpak.user_id = d['user_id']
        pikpak.device_id = d['device_id']
        return pikpak
    
    async def get_quota_info_v1(self, pikpak:PikPakApi, retry = 1):
        for i in range(retry):
            try:
                return await pikpak.get_quota_info()
            except:
                pass

    async def update_login_data(self, data : pd.DataFrame):
        pikpak_clients =await asyncio.gather(*[self.initialize_pikpak(uname) for uname in data['username'].drop_duplicates().to_list()])
        for uname in data['username'].drop_duplicates().to_list():
            pikpak_client = [pikpak_client for pikpak_client in pikpak_clients if pikpak_client.username == uname][0]
            data.loc[data.username == uname, 'encoded_token'] = pikpak_client.encoded_token
            data.loc[data.username == uname, 'access_token'] = pikpak_client.access_token
            data.loc[data.username == uname, 'refresh_token'] = pikpak_client.refresh_token
            #quota = await self.get_quota_info_v1(pikpak_client)
            #free_space = (int(quota['quota']['limit']) - int(quota['quota']['usage']))/ (1024**2)
            #pikpak_client.free_space = free_space
            data.loc[data.username == uname, 'login_time'] = str(time.time())
        return data
    
    async def get_stream_using_db(self,data):
        s_tasks = []
        for index, d in data.iterrows():
            pikpak_client = self.get_pikpak_client(d)
            s_tasks.append(self.get_stream(d['file_id'], d['filename'], pikpak_client))
            
        file_infos = await asyncio.gather(*s_tasks)
        return file_infos
    
    def get_responce_formatted(self, file_infos : list, data : pd.DataFrame):
        pikpak_streams = []
        for i in range(len(file_infos)):
            pikpak_streams.append({'name' : '📽️'+ data['quality'][i], 
                                   'description' : data['title'][i],
                                   'url' : self.get_stream_url(file_infos[i]),})
        return {'streams' : pikpak_streams}
    def get_stream_url(self, stream):
        return [x['link'] for x in stream['medias'] if 'link' in x.keys()][0]['url']
    
    async def pikpak_main(self, imdb_id, type):
        
        print(f"Requested {type} {imdb_id}")
        start_time = time.time()
        
        ### Fetching Torrents
        torrents_in = self.get_torrents(imdb_id, type) if type == 'movie' else self.get_series_torrents(imdb_id)
        torrents = torrents_in.copy()
        for torr in torrents:
            if 'filename' not in torr['behaviorHints'].keys():
                torr['behaviorHints']['filename'] = ''
        if len(torrents) == 0:
            print("--- completed in %s seconds ---" % (time.time() - start_time))
            return {"streams" : []}
        
        print("--- Fetched torrents in %s seconds ---" % (time.time() - start_time))
        
        ### Check Existing Downloads
        data = self.get_data_from_db_pg(imdb_id)
        data_torrents = self.get_torrent_data_from_db_pg(imdb_id)
        
        print("--- Fetched imdb data from db in %s seconds ---" % (time.time() - start_time))
        
        if len(data.index) > 0 and len(torrents) == len(data_torrents.index):
            if time.time() - float(data['login_time'][0]) > 7000:
                self.clear_record_with_imdb(data['imdb_id'][0])
                data = await self.update_login_data(data)
                self.write_dataframe_to_db(data)
                self.file_infos = await self.get_stream_using_db(data)
                print("--- completed in %s seconds ---" % (time.time() - start_time))
                return self.get_responce_formatted(self.file_infos, data)
            else:
                self.file_infos = await self.get_stream_using_db(data)
                print("--- completed in %s seconds ---" % (time.time() - start_time))
                return self.get_responce_formatted(self.file_infos, data)
        
        elif len(data.index) > 0:
            print("--- Got New Torrents ---")
            self.clear_record_with_imdb(data['imdb_id'][0])
            self.clear_torrent_with_imdb(data['imdb_id'][0])

        ### Check existing Series infoHash in DB
        
        quality_added = []
        old_task = []

        if type == 'series':
            data_series = self.get_series_from_db_pg(imdb_id)
            existing_infoHash_list = [x for x in data_series.infoHash.drop_duplicates() if x in [x['infoHash'] for x in torrents]]
            
            if len(existing_infoHash_list) > 0 :
                existing_infoHash_list_map = [{"infoHash" : y, "filename" : x['behaviorHints']['filename'], "title" : x['title'] } for y in existing_infoHash_list for x in torrents if x['infoHash']== y]
                data_series = data_series[data_series['infoHash'].isin(existing_infoHash_list)].copy()
                
                temp = {}
                for _, x in data_series.iterrows():
                    temp[x.infoHash] = x.to_dict()
                
                data_series = pd.DataFrame(temp).T
                
                for infoH in existing_infoHash_list_map:
                    data_series.loc[data_series['infoHash'] == infoH['infoHash'], 'title'] = infoH['title']
                    data_series.loc[data_series['infoHash'] == infoH['infoHash'], 'filename'] = infoH['filename']
                    data_series.loc[data_series['infoHash'] == infoH['infoHash'], 'infoHash'] = infoH['infoHash'] + "_"
                
                if time.time() - float(data_series['login_time'].values[0]) > 7000:
                    data_series = await self.update_login_data(data_series)
                
                for quality in data_series.quality:
                    quality_added.append("Torrentio\n"+quality)
                
                for index, row in data_series.iterrows():
                    old_task.append({"type": type, "imdb_id": imdb_id, "quality": row['quality'], "title": row['title'], 
                                    "behaviorHints": {"filename": row['filename']}, "infoHash": row['infoHash'],
                                    "task": {"file_id" : row['base_file_id']}, "pikpak_client" : self.get_pikpak_client(row)})

        ### New Download Task --------------------------------------
        self.valid_tasks = []
        quality_f = None

        self.task_ids = []
        if self.pikpak_clients == None or len(self.pikpak_clients) == 0 :
            self.pikpak_clients = await self.get_new_pikpak_client(10)
        elif (time.time() - self.pikpak_clients[0].login_time) > 7000:
            self.pikpak_clients = await self.get_new_pikpak_client(10)
        else:
            if len(self.pikpak_clients) < 9:
                self.pikpak_clients.extend(await self.get_new_pikpak_client(10-len(self.pikpak_clients)))
        
        print("--- Login Completed in %s seconds ---" % (time.time() - start_time))

        ### Considering batches of torrents
        for batch_torrents in self.get_next_torr(torrents):
            if quality_f == None:
                quality_f = [torr['name'] for torr in batch_torrents]
            
            batch_torrents = [torr for torr in batch_torrents if torr['name'] not in quality_added]
            
            if len(batch_torrents) == 0:
                break

            ### Adding Task
            #magnets = [f"magnet:?xt=urn:btih:{x['infoHash']}" for x in  batch_torrents]

            magnets = []
            sizes = []
            tasks = []
            b_magnets = []
            pikpak_clients_used = []
            for x in batch_torrents:
                if sum(sizes) + x['size'] < 6100:
                    magnets.append(f"magnet:?xt=urn:btih:{x['infoHash']}")
                    sizes.append(x['size'])
                    continue
                else:
                    b_magnets.append(magnets)
                    magnets = []
                    sizes = []
                    magnets.append(f"magnet:?xt=urn:btih:{x['infoHash']}")
                    sizes.append(x['size'])
            b_magnets.append(magnets)

            b_tasks = []
            
            for magnets in b_magnets:
                if len(self.pikpak_clients) == 0:
                    self.pikpak_clients = await self.get_new_pikpak_client(10)
                pikpak_client = self.get_pikpak_client(vars(self.pikpak_clients[0]))
                self.pikpak_clients = self.pikpak_clients[1:]
                for magnet in magnets:
                    pikpak_clients_used.append(pikpak_client)
                b_tasks.extend([self.get_with_timeout(pikpak_client.offline_download(x)) for x in magnets])
            tasks = await asyncio.gather(*b_tasks)

            print("--- Download Tasks Added in %s seconds ---" % (time.time() - start_time))

            for i in range(len(tasks)):
                if tasks[i] == None:
                        self.update_email_used(pikpak_clients_used[i].username)
                        continue
                else:
                    tasks[i]['email'] = pikpak_clients_used[i].username
                    tasks[i]['pikpak_client'] = pikpak_clients_used[i]
                    self.update_email_used(pikpak_clients_used[i].username)
                    
            
            vidx = []
            for i in range(len(tasks)):
                if tasks[i] !=None:
                    vidx.append(i)

            self.task_ids.extend([task['task']['id'] for task in tasks if task !=None])
            
            ### Checking Valid Tasks with file_ID and adding additional info with imdb id and quality
            for i in vidx:
                quality = batch_torrents[i]['name']
                if tasks[i]['task']['file_id'] != '' and quality not in quality_added:
                    quality_added.append(quality)
                    self.valid_tasks.append({'email' : tasks[i]['email']} | { "type" : type } | 
                                            { "imdb_id" : imdb_id } | 
                                            {'quality' : quality.split("\n")[-1]} | 
                                            batch_torrents[i] | tasks[i])
    
            ### Break if all qualities added
            if len(self.valid_tasks) == len(quality_f):
                break
        
        ### Check if no valid tasks found
        if len(self.valid_tasks) == 0 and len(old_task) == 0:
            return {"streams" : []}
        
        ### Waiting for download to complete fully
        completed_task, isAllComplete = await self.wait_for_task_complete(self.valid_tasks, 15)

        print("--- Download Tasks Completed in %s seconds ---" % (time.time() - start_time))

        ### Filter Completed Tasks
        self.valid_tasks = [x for x in self.valid_tasks if x['task']['id'] in completed_task]
        
        ### Extend old task if any
        self.valid_tasks.extend(old_task)

        ### Getting Stream URL and File Info
        self.file_infos = await asyncio.gather( *[self.get_stream(task['task']['file_id'], task['behaviorHints']['filename'], task['pikpak_client']) for task in self.valid_tasks])
        
        print("--- Got Stream URLs in %s seconds ---" % (time.time() - start_time))

        ### Adding file_info to tasks
        for i in range(len(self.valid_tasks)):
            self.valid_tasks[i] = self.valid_tasks[i] | self.file_infos[i]

        ### Return output streams
        pikpak_streams = []
        for stream in self.valid_tasks:
            pikpak_streams.append({'name' : '📽️'+ stream['quality'], 
                                   'description' : stream['title'],
                                   'url' : self.get_stream_url(stream),})
        
        ### update db and task cleanup
        self.write_torrent_to_DB_pg(torrents_in, imdb_id, type)
        await self.post_responce_cleanup()
        print("--- completed in %s seconds ---" % (time.time() - start_time))
        return {"streams" : pikpak_streams}

if __name__ == "__main__":

    pp = pradhanStreams()
    asyncio.run(pp.pikpak_main('tt6263850', 'movie'))