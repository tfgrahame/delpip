#!/usr/bin/env python3

import aiohttp
import asyncio
import os
import sqlite3
import ssl
import sys

entity_type = sys.argv[1]
entity_map = {'contributor': 'people'}

nitro_url = os.environ.get('NITRO_BASE') + entity_map[entity_type]

ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
ssl_context.load_cert_chain(os.environ.get('CERT'))
conn = aiohttp.TCPConnector(ssl_context=ssl_context)
max_tasks = 2

async def read_pid(db_conn):
    c = db_conn.cursor()
    pid = [row[0] for row in c.execute("SELECT pid FROM contributors WHERE processing = '0' AND deleted = '0' LIMIT 1")]
    if pid[0]:
        print("{0} pid found".format(pid[0]))
        return pid[0]
    else:
        return None

async def mark_processing(db_conn):
    c = db_conn.cursor()
    pid = await read_pid(db_conn)
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
    pass

async def fetch_pip(session, pid):
    print("fetching pid {0}".format(pid))
    url = os.environ.get('PIPS_BASE') + entity_type + '/pid.' + pid + '?format=json'
    print(url)
    async with session.get(url, proxy=os.environ.get('http_proxy')) as response:
        return await response.json()

async def check_nitro(pid, session):
    print("checking Nitro with {0}".format(pid))
    url = os.environ.get('NITRO_BASE') + entity_map[entity_type] + '?pid=' + pid + '&api_key=' +  os.environ.get('NITRO_KEY')
    async with session.get(url, proxy=os.environ.get('http_proxy')) as response:
        return response.status

async def process_pip(db_conn, lock, session):
    print("Processing pip ...")
    await lock.acquire()
    pid = await mark_processing(db_conn)
    lock.release()
    response = await fetch_pip(session, pid)
    print(response[1]['pid'])
    checkresult = await check_nitro(pid, session)
    print(checkresult)

def main():
    db_conn = sqlite3.connect('db')
    with aiohttp.ClientSession(connector=conn) as session:
        lock = asyncio.Lock()
        loop = asyncio.get_event_loop()
        tasks = [loop.create_task(process_pip(db_conn, lock, session)) for task in range(max_tasks)]
        loop.run_until_complete(asyncio.wait(tasks))
    db_conn.close()

if __name__ == "__main__":
    main()
