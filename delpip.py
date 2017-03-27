#!/usr/bin/env python3

import aiohttp
import asyncio
import os
import sqlite3
import ssl
import sys

entity_type = sys.argv[1]
entity_map = {'contributors': 'people'}

pips_url = os.environ.get('PIPS_BASE') + entity_type + '/pid.'
nitro_url = os.environ.get('NITRO_BASE') + entity_map[entity_type]

ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
ssl_context.load_cert_chain(os.environ.get('CERT'))
conn = aiohttp.TCPConnector(ssl_context=ssl_context)
max_tasks = 5

def read_pid(db_conn):
    c = db_conn.cursor()
    pid = [row[0] for row in c.execute("SELECT pid FROM contributors WHERE processing = '0' AND deleted = '0' LIMIT 1")]
    if pid[0]:
        return pid[0]
    else:
        return None

def mark_processing(db_conn, pid):
    c = db_conn.cursor()
    p = (pid,)
    c.execute("UPDATE contributors SET processing = '1' WHERE pid = ?", p)
    db_conn.commit()

def mark_deleted(db_conn, pid):
    c = db_conn.cursor()
    p = (pid,)
    c.execute("UPDATE contributors SET deleted = '1' WHERE pid = ?", p)
    db_conn.commit()

def main():
    db_conn = sqlite3.connect('db')
    conn.close()

if __name__ == "__main__":
    main()
