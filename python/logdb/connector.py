#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import string
import httplib
import hashlib
import socket
import threading
import uuid

class TrapReceiver(threading.Thread):
    def __init__(self, host, cookie):
        threading.Thread.__init__(self)
        self.do_stop = False
        self.listeners = []
        self.host = host
        self.cookie = cookie
    
    def run(self):
        self.do_stop = False
        while not self.do_stop:
            self.recv_trap()
        
    def add_listener(self, listener):
        self.listeners.append(listener)
        
    def remove_listener(self, listener):
        self.listeners.remove(listener)

    def recv_trap(self):
        trapconn = httplib.HTTPConnection(self.host, timeout=5000)
        headers = {"Content-Type": "text/json" }
        if hasattr(self, 'cookie'):
            headers['Cookie'] = self.cookie
        
        try:
            trapconn.request('GET', '/msgbus/trap', None, headers)
            trapconn.sock.settimeout(5)
            resp = trapconn.getresponse(False)
            resp_json = resp.read()
            o = json.loads(resp_json)
            
            traps = [Message.parse(x) for x in o ]
            for trap in traps:
                for listener in self.listeners:
                    listener.ontrap(trap)
        except (httplib.HTTPResponse, socket.error):
            pass
        
        trapconn.close()
    
    def close(self):
        self.do_stop = True
        self.join(10)
        

class Session:
    def __init__(self, host):
        self.host = host
        self.conn = httplib.HTTPConnection(host)
        self.listeners = []

    def close(self):
        if hasattr(self, 'trap'):
            self.trap.close()
            
        self.conn.close()

    def login(self, nick, password, force=False):
        # say hello and get nonce
        hello = self.rpc("org.krakenapps.dom.msgbus.LoginPlugin.hello")
        nonce = hello.params['nonce']
        h = self.hash_password(password, nonce)
        
        # try login
        login = self.rpc('org.krakenapps.dom.msgbus.LoginPlugin.login', { "nick": nick, "hash": h, "force": force })

        # start trap loop
        self.trap = TrapReceiver(self.host, self.cookie)
        self.trap.add_listener(self)
        self.trap.start()
        
    def logout(self):
        self.rpc('org.krakenapps.dom.msgbus.LoginPlugin.logout')

    def hash_password(self, password, nonce):
        ph = hashlib.sha1()
        ph.update(password)
         
        m = hashlib.sha1()
        m.update(ph.hexdigest() + nonce)
        return m.hexdigest()

    def rpc(self, method, params=None):
        if params is None:
            params = {}
        
        body = json.dumps([{ 
           "guid": str(uuid.uuid1()),
           "type": "Request",
           "source": "0",
           "target": "0",
           "method": method  
           }, params ])
     
        headers = {"Content-Type": "text/json" }
        if hasattr(self, 'cookie'):
            headers['Cookie'] = self.cookie
            
        self.conn.request('POST', '/msgbus/request', body, headers)
        resp = self.conn.getresponse(False)
        cookie = resp.getheader('set-cookie')
        if cookie != None:
            self.cookie = cookie.split(';')[0]
            
        resp_json = resp.read()
        o = json.loads(resp_json)
        m = Message.parse(o)
        if m.errcode is not None:
            raise MessageException(m.errcode, m.errmsg, {})

        return m
    
    def register_trap(self, callback_name):
        self.rpc('org.krakenapps.msgbus.PushPlugin.subscribe', { 'callback': callback_name })

    def unregister_trap(self, callback_name):
        self.rpc('org.krakenapps.msgbus.PushPlugin.unsubscribe', { 'callback': callback_name })

    def add_listener(self, listener):
        self.listeners.append(listener)
        
    def remove_listener(self, listener):
        self.listeners.remove(listener)
        
    def ontrap(self, trap):
        for listener in self.listeners:
            listener.ontrap(trap)

class Message:
    def __init__(self):
        pass    
    
    @staticmethod
    def parse(json):
        m = Message()
        m.guid = json[0]['guid']
        m.type = json[0]['type']
        m.method = json[0]['method']
        m.errcode = None
        m.errmsg = None
        if json[0].has_key('errorCode'):
            m.errcode = json[0]['errorCode']
            m.errmsg = json[0]['errorMessage']
        m.params = json[1]
        return m

    def __repr__(self):
        return self.type + ", " + self.method + ": " + str(self.params)

class MessageException(Exception):
    def __init__(self, code, body, params):
        self.code = code
        self.msg = body
        self.params = params
    
    def __str__(self):
        return repr(self)

    def __repr__(self):
        if self.msg:
            return 'Msgbus Exception> ' + self.code + ': ' + self.msg
        return 'Msgbus Exception> ' + self.code
    
    
class LogQuery:
    def __init__(self, query_id, query_string):
        self.id = query_id
        self.query_string = query_string
        self.status = 'Stopped'
        self.loaded_count = 0
        self.waiting_conds = []
        
    def update_count(self, count):
        self.loaded_count = count
        for cond in self.waiting_conds:
            threshold = cond[0]
            if threshold and threshold <= self.loaded_count:
                with cond[1]:
                    cond[1].notify_all()
                    
    def update_status(self, status):
        self.status = status
        if status == 'Ended':
            for cond in self.waiting_conds:
                with cond[1]:
                    cond[1].notify_all()
        
    def __repr__(self):
        return '[' + str(self.id) + '|' + self.status + '] ' + self.query_string + ", loaded_count=" + str(self.loaded_count)
    
class LogDbConnector:
    def __init__(self):
        self.queries = {}
    
    def connect(self, host, nick, password):
        self.session = Session(host)
        self.session.login(nick, password, True)
        self.session.add_listener(self)
        
    def open_cursor(self, query_id, offset, limit, close=False):
        cached = None
        current_cache_offset = None
        next_cache_offset = offset
        fetch_unit = 1000
        try:
            for i in xrange(offset, offset + limit):
                if not cached or i >= current_cache_offset + fetch_unit:
                    cached = self.get_result(query_id, next_cache_offset, fetch_unit)
                    current_cache_offset = next_cache_offset 
                    next_cache_offset = next_cache_offset + fetch_unit
                
                relative = i - current_cache_offset
                if relative >= len(cached['result']):
                    break
                yield cached['result'][relative]
        finally:
            if close:
    	        self.remove_query(query_id)
        
    def query(self, query_string):
        query_id = self.create_query(query_string)

        self.start_query(query_id)
        self.wait_until(query_id, None)
        total = self.queries[query_id].loaded_count
    
        return self.open_cursor(query_id, 0, total, True)
    
    def create_query(self, query_string):
        resp = self.session.rpc('org.krakenapps.logdb.msgbus.LogQueryPlugin.createQuery', {'query': query_string })
        query_id = resp.params['id']
        self.session.register_trap('logstorage-query-' + str(query_id))
        self.session.register_trap('logstorage-query-timeline-' + str(query_id))
        
        self.queries[query_id] = LogQuery(query_id, query_string) 
        return query_id
    
    def start_query(self, query_id, page_size=10, timeline_size=10):
        if query_id not in self.queries:
            raise Exception('query ' + str(query_id) + ' does not exist')

        self.session.rpc('org.krakenapps.logdb.msgbus.LogQueryPlugin.startQuery',
                         {'id':query_id, 'offset': 0, 'limit': page_size, 'timeline_limit': timeline_size})
        self.queries[query_id].update_status('Running')
        
    def stop_query(self, query_id):
        if query_id not in self.queries:
            raise Exception('query ' + str(query_id) + ' does not exist')

        self.session.rpc('org.krakenapps.logdb.msgbus.LogQueryPlugin.stopQuery', {'id': query_id})

    def remove_query(self, query_id):
        if query_id not in self.queries:
            raise Exception('query ' + str(query_id) + ' does not exist')

        self.session.unregister_trap('logstorage-query-' + str(query_id))
        self.session.unregister_trap('logstorage-query-timeline-' + str(query_id))
        self.session.rpc('org.krakenapps.logdb.msgbus.LogQueryPlugin.removeQuery', {'id': query_id })
        del self.queries[query_id]

    def wait_until(self, query_id, count):
        cv = threading.Condition()
        self.queries[query_id].waiting_conds.append([count, cv])
        with cv:
            cv.wait()
        
    def get_result(self, query_id, offset, limit):
        if query_id not in self.queries:
            raise Exception('query ' + str(query_id) + ' does not exist')

        resp = self.session.rpc('org.krakenapps.logdb.msgbus.LogQueryPlugin.getResult',
                                {'id': query_id, 'offset': offset, 'limit': limit})
        if len(resp.params) == 0:
            raise MessageException('query-not-found', '', resp.params)
        
        return resp.params
    
    def close(self):
        self.session.close()

    def ontrap(self, trap):
        p = trap.params
        if 'logstorage-query-timeline' in trap.method:
            self.queries[p['id']].update_count(p['count']) 
            if p['type'] == u'eof':
                self.queries[p['id']].update_count(p['count'])
                self.queries[p['id']].update_status('Ended')

        elif 'logstorage-query' in trap.method:
            if p['type'] == u'eof':
                self.queries[p['id']].update_count(p['total_count'])
                self.queries[p['id']].update_status('Ended')
