#!/usr/bin/env python3

import aiohttp
import asyncio
import os
import sqlite3
import ssl
import sys

entity_type = sys.argv[1]
entity_map = {'contributor': 'people'}

ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
ssl_context.load_cert_chain(os.environ.get('CERT'))
conn = aiohttp.TCPConnector(ssl_context=ssl_context)
max_workers = 1

def read_pid(db_conn):
    c = db_conn.cursor()
    pid = [row[0] for row in c.execute("SELECT pid FROM contributors WHERE processing = '0' AND deleted = '0' LIMIT 1")]
    if pid[0]:
        print("{0} pid found".format(pid[0]))
        return pid[0]
    else:
        return None

def mark_processing(db_conn):
    c = db_conn.cursor()
    pid = read_pid(db_conn)
    if pid != None:
        p = (pid,)
        c.execute("UPDATE contributors SET processing = '1' WHERE pid = ?", p)
        db_conn.commit()
        print("{0} marked for processing".format(pid))
        return pid
    else:
        return None

def mark_deleted(db_conn, pid):
    c = db_conn.cursor()
    p = (pid,)
    c.execute("UPDATE contributors SET deleted = '1' WHERE pid = ?", p)
    db_conn.commit()

async def delete_pip(session, pid):
    print("deleting pid {0}".format(pid))
    url = os.environ.get('PIPS_BASE') + entity_type + '/pid.' + pid
    print(url)
    async with session.delete(url, proxy=os.environ.get('http_proxy')) as response:
        return response.status

async def pip_in_nitro(pid, session):
    await asyncio.sleep(60)
    print("seeing if {0} is still in Nitro ...".format(pid))
    headers = {"Accept": "application/json"}
    url = os.environ.get('NITRO_BASE') + entity_map[entity_type] + '?pid=' + pid + '&api_key=' +  os.environ.get('NITRO_KEY')
    async with session.get(url, proxy=os.environ.get('http_proxy'), headers=headers) as response:
        data = await response.json()
        if data['nitro']['results']['total'] != 0:
            return True
        else:
            return False

async def reader(db_conn, q):
    while True:
        pid = mark_processing(db_conn)
        if pid != None:
            print("putting {0}".format(pid))
            await q.put(pid)
        else:
            for i in range(max_workers):
                q.put(None)

async def worker(db_conn, session, q):
    print("worker started ... ")
    while True:
        item = await q.get()
        print("got {0}".format(item))
        if item == None:
            q.task_done()
            break
        else:
            delete_status = await delete_pip(session, item)
            pip_exists = True
            while pip_exists:
                pip_exists = await pip_in_nitro(item, session)
            q.task_done()
        mark_deleted(db_conn, item)

def main():
    db_conn = sqlite3.connect('db')
    with aiohttp.ClientSession(connector=conn) as session:
        q = asyncio.Queue(maxsize = 2)
        loop = asyncio.get_event_loop()
        reader_task = loop.create_task(reader(db_conn, q))
        worker_tasks = [loop.create_task(worker(db_conn, session, q)) for i in range(max_workers)]
        loop.run_until_complete(asyncio.wait([reader_task] + worker_tasks))
    db_conn.close()

if __name__ == "__main__":
    main()
