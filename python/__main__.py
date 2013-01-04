#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logdb import connector
import string
import socket

class Console:
    
    def __init__(self):
        self.logdb = None
		
    def help(self):
        print 'connect <host> <loginname> <password>'
        print '\tconnect to specified kraken logdb instance'
        
        print 'disconnect'
        print '\tdisconnect database connection'
        
        print 'queries'
        print '\tprint all queries initiated by this session'
        
        print 'query <query string>'
        print '\tcreate, start and fetch query result at once'
        
        print 'create_query <query string>'
        print '\tcreate query with specified query string, and return allocated query id'
        
        print 'start_query <query id>'
        print '\tstart query'
        
        print 'stop_query <query_id>'
        print '\tstop running query'
        
        print 'remove_query <query_id>'
        print '\tstop and remove query'
        
        print 'fetch <query_id> <offset> <limit>'
        print '\tfetch result set of specifeid window. you can fetch partial result before query is ended'

    def connect(self, tokens):
        if len(tokens) < 4:
            print 'Usage: connect <host> <loginname> <password>'
            return
        
        if self.logdb:
            print 'already connected'
            return
        
        self.host = tokens[1]
        try:
            socket.gethostbyname(tokens[1])
        except Exception as e:
            print 'invalid hostname ' + tokens[1] + ', connect failed'
            print e
            return

        login_name = tokens[2]
        password = tokens[3]
        try:
            self.logdb = connector.LogDbConnector()
            self.logdb.connect(self.host, login_name, password)
            print 'connected to ' + self.host + ' as ' + login_name
        except Exception as ex:
            print ex
            if self.logdb:
                self.logdb.close()
                self.logdb = None

    def disconnect(self):
        if not self.logdb:
            print 'not connected yet'
            return
        
        print 'closing connection...'
        self.logdb.close()
        print 'disconnected'
        self.logdb = None
        
    def queries(self):
        if not self.logdb:
            print 'connect first please'
            return

        if len(self.logdb.queries) == 0:
            print 'no result'
            return

        for q in self.logdb.queries.items():
            print q
        
    def query(self, tokens):
        if not self.logdb:
            print 'connect first please'
            return
        
        query_string = string.join(tokens[1:])
        print 'querying [' + query_string + "] ..."
        count = 0
        for x in self.logdb.query(query_string):
            print x
            count = count + 1

        print 'total ' + str(count) + ' row(s)'
            
    def create_query(self, tokens):
        if not self.logdb:
            print 'connect first please'
            return
        
        if len(tokens) < 2:
            print 'Usage: create_query <query_string>'
            return
        
        try:
            query_string = string.join(tokens[1:])
            query_id = self.logdb.create_query(query_string)
            print 'created query ' + str(query_id)
        except Exception as e:
            print e   
			
    def start_query(self, tokens):
        if not self.logdb:
            print 'connect first please'
            return
        
        if len(tokens) < 2:
            print 'Usage: create_query <query_id>'
            return
        
        try:
            query_id = int(tokens[1])
            self.logdb.start_query(query_id)
            print 'started query ' + str(query_id)
        except Exception as e:
            print e   
        
    def stop_query(self, tokens):
        if not self.logdb:
            print 'connect first please'
            return
        
        if len(tokens) < 2:
            print 'Usage: stop_query <query_id>'
            return

        try:
            query_id = int(tokens[1])
            self.logdb.stop_query(query_id)
            print 'stopped query ' + str(query_id)
        except Exception as e:
            print e   
    
    def remove_query(self, tokens):
        if not self.logdb:
            print 'connect first please'
            return

        if len(tokens) < 2:
            print 'Usage: remove_query <query_id>'
            return

        try:
            query_id = int(tokens[1])
            self.logdb.remove_query(query_id)
            print 'removed query ' + str(query_id)
        except Exception as e:
            print e   
        
    def fetch(self, tokens):
        if not self.logdb:
            print 'connect first please'
            return

        if len(tokens) < 4:
            print 'Usage: fetch <query_id> <offset> <limit>'
            return
        
        query_id = int(tokens[1])
        offset = int(tokens[2])
        limit = int(tokens[3])

        count = 0
        try:
            page = self.logdb.get_result(query_id, offset, limit)
            for x in page['result']:
                print x
                count = count + 1
            print str(count) + ' row(s)'
        except Exception as e:
            print e   

    def prompt(self):
        if self.logdb:
		    return 'logdb@' + self.host + '> '
        return 'logdb> '

    def run(self):
        print 'Kraken LogDB Console 0.1 (2013-01-04)'
        print 'Type "help" for more information'
        try:
            while True:
                try:
                    line = raw_input(self.prompt())
                    tokens = line.split()
                    if len(tokens) == 0:
                        continue
            
                    cmd = tokens[0]
                    if cmd == 'quit' or cmd == 'exit':
                        break				
                    if cmd == 'help':
    	    			self.help()
                    elif cmd == 'connect':
                        self.connect(tokens)
                    elif cmd == 'disconnect':
                        self.disconnect()
                    elif cmd == 'query':
                        self.query(tokens)
                    elif cmd == 'create_query':    
                        self.create_query(tokens)
                    elif cmd == 'start_query':
                        self.start_query(tokens)
                    elif cmd == 'stop_query':
                        self.stop_query(tokens)
                    elif cmd == 'remove_query':
                        self.remove_query(tokens)
                    elif cmd == 'fetch':
                        self.fetch(tokens)
                    elif cmd == 'queries':
                        self.queries()
                    else:
                        print 'syntax error'
                except:
                    print ''
    	            continue
        finally:
            if self.logdb:
                print 'closing logdb connection...'
                self.logdb.close()

Console().run()
print 'bye!'