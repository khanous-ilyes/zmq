from __future__ import print_function
import os
from threading import Thread
import binascii
import codecs
import zmq
import sqlite3
from zhelpers import socket_set_hwm, zpipe
import random
file = open("test", "rb")
file.seek(0,os.SEEK_END)
CHUNK_SIZE =int(file.tell()/4)
print(CHUNK_SIZE)
print(file.tell())
offset1=0
offset2=CHUNK_SIZE
offset3=CHUNK_SIZE*2
offset4=CHUNK_SIZE*3
def getChunks(idy):
        # return chunks [ch1, ch2]
        conndb = sqlite3.connect('data1.db')
        cur = conndb.cursor()
        cur.execute('select num,stream1,stream2 from data1 where id = ?', (idy,))
        ret = cur.fetchone()
        conndb.close()
        if ret :
            return ret[0], [ret[1] , ret[2]]
        else :
            return None, None
def server_thread1(ctx):
    file = open("test", "rb")

    router = ctx.socket(zmq.ROUTER)
    b=3
    router.bind("tcp://*:5000")
    
    
    x='test1'
    while True:
        # First frame in each message is the sender identity
        # Second frame is "fetch" command
        try:
            msg = router.recv_multipart()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                return   # shutting down, quit
            else:
                raise

        identity, command, offset_str, chunksz_str = msg
        chunksz=int(chunksz_str )
        idy=int.from_bytes(identity,"little")
       
        assert command == b"fetch"
        if b==3 :
             # Read chunk of data from file
            # Send resulting chunk to client
        
            router.send_multipart([identity, x.encode('utf-8')])
            b=2
        if b==2 :
            file.seek(offset1,0)
            data = file.read(chunksz)    
            router.send_multipart([identity, data])
            b=1
        if b==1 :  
         # Read chunk of data from file
            file.seek(offset2,0)
            #print("current position is %i"%file.seek(offset2))
            data = file.read(chunksz)
            b=0
            # Send resulting chunk to client
            router.send_multipart([identity, data])
        if b==0:
           break
def server_thread2(ctx):
    file = open("test", "rb")

    router = ctx.socket(zmq.ROUTER)
    b=3
    router.bind("tcp://*:6000")
    
    
    x='test2'
    while True:
        # First frame in each message is the sender identity
        # Second frame is "fetch" command
        try:
            msg = router.recv_multipart()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                return   # shutting down, quit
            else:
                raise

        identity, command, offset_str, chunksz_str = msg
        chunksz=int(chunksz_str )
        idy=int.from_bytes(identity,"little")
       
        assert command == b"fetch"
        if b==3 :
             # Read chunk of data from file
            # Send resulting chunk to client
        
            router.send_multipart([identity, x.encode('utf-8')])
            b=2
        if b==2 :
            file.seek(offset2,0)
            data = file.read(chunksz)    
            router.send_multipart([identity, data])
            b=1
        if b==1 :  
         # Read chunk of data from file
            file.seek(offset3,0)
            #print("current position is %i"%file.seek(6))
            data = file.read(chunksz)
            b=0
            # Send resulting chunk to client
            router.send_multipart([identity, data])
        if b==0:
           break
def server_thread3(ctx):
    file = open("test", "rb")

    router = ctx.socket(zmq.ROUTER)
    b=3
    router.bind("tcp://*:7000")
    
    
    x='test3'
    while True:
        # First frame in each message is the sender identity
        # Second frame is "fetch" command
        try:
            msg = router.recv_multipart()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                return   # shutting down, quit
            else:
                raise

        identity, command, offset_str, chunksz_str = msg
        chunksz=int(chunksz_str )
        idy=int.from_bytes(identity,"little")
       
        assert command == b"fetch"
        if b==3 :
             # Read chunk of data from file
            # Send resulting chunk to client
        
            router.send_multipart([identity, x.encode('utf-8')])
            b=2
        if b==2 :
            file.seek(offset3,0)
            data = file.read(chunksz)    
            router.send_multipart([identity, data])
            b=1
        if b==1 :  
         # Read chunk of data from file
            file.seek(offset4,0)
            #print("current position is %i"%file.seek(6))
            data = file.read(chunksz)
            b=0
            # Send resulting chunk to client
            router.send_multipart([identity, data])
        if b==0:
           break           
def server_thread4(ctx):
    file = open("test", "rb")

    router = ctx.socket(zmq.ROUTER)
    b=3
    router.bind("tcp://*:8000")
    
    
    x='test4'
    while True:
        # First frame in each message is the sender identity
        # Second frame is "fetch" command
        try:
            msg = router.recv_multipart()
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                return   # shutting down, quit
            else:
                raise

        identity, command, offset_str, chunksz_str = msg
        chunksz=int(chunksz_str )
        idy=int.from_bytes(identity,"little")
       
        assert command == b"fetch"
        if b==3 :
             # Read chunk of data from file
            # Send resulting chunk to client
        
            router.send_multipart([identity, x.encode('utf-8')])
            b=2
        if b==2 :
            file.seek(offset4,0)
            data = file.read(chunksz)    
            router.send_multipart([identity, data])
            b=1
        if b==1 :  
         # Read chunk of data from file
            file.seek(offset1,0)
            #print("current position is %i"%file.seek(6))
            data = file.read(chunksz)
            b=0
            # Send resulting chunk to client
            router.send_multipart([identity, data])
        if b==0:
           break

# The main task is just the same as in the first model.
# .skip

def main():

    # Start child threads
    ctx = zmq.Context()
    server_thread1(ctx)
    server_thread2(ctx)
    server_thread3(ctx)
    server_thread4(ctx)
    #a = ctx.socket(zmq.PAIR)
    #a.linger =0
    #a.hwm  = 1
    #iface = "inproc://%s" % binascii.hexlify(b'RA\xe3\xa7e\x0e:\xc3')
    #a.bind(iface)
    #CreateDatabase()
    
    #server1 = Thread(target=server_thread1, args=(ctx,))
   
    #server1.start()
    #num,d1,d2=getChunks(testdata)
    #print(num)
    #print(d1)
    #print(d2)   
    # loop until client tells us it's done
    #try:
        #print (a.recv())
       
    #except KeyboardInterrupt:
        #pass
    #del a
    #ctx.term()

if __name__ == '__main__':
    main()

