from __future__ import print_function
import os
from threading import Thread
import codecs
import zmq
import binascii
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
database=[]
database1=[]
def CreateDatabase():
        if not (os.path.exists("da.db")) :
            conndb = sqlite3.connect('da.db')
            cur = conndb.cursor()
            cur.execute("""CREATE TABLE "da" (
                            "id"	TEXT NOT NULL UNIQUE,
                            "num"	TEXT,
                            "stream1"	TEXT,
                            "stream2"	TEXT
                            )
                        """)
            conndb.commit()
            conndb.close()
def save( idy, num="", data=["", ""]):
        try:
            conndb = sqlite3.connect('da.db')
            cur = conndb.cursor()
            cur.execute("SELECT id,num FROM da WHERE id = ?", (idy,))
            if not cur.fetchone() :
                cur.execute('insert into da values(?,?,?,?)', (idy, num, data[0], data[1]))
                conndb.commit()
                print("succefuly identifier saved id : " + idy)
            elif len(num) != 0:
                cur.execute('UPDATE da SET (num,stream1,stream2)=(?,?,?) WHERE id = ?', (num, data[0], data[1], idy,))
                conndb.commit()
                print("succefuly saved entire chunks number : " +num)
            conndb.close()
        except:
            print("")

def client_thread1(ctx):
    dealer = ctx.socket(zmq.DEALER)
    socket_set_hwm(dealer, 1)
    dealer.connect("tcp://127.0.0.1:8000")

    total = 0       # Total bytes received
    chunks = 0      # Total chunks received
    boo=3
    while True:
        # ask for next chunk
        dealer.send_multipart([
            b"fetch",
            b"%i" % total,
            b"%i" % CHUNK_SIZE
        ])

        try:
            chunk = dealer.recv()
            boo-=1
        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                return   # shutting down, quit
            else:
                raise
        database1.append(chunk)
        chunks += 1
        size = len(chunk)
        total += size
        if boo==0:
            break   # Last chunk received; exit
    print(database1[0])
    print(database1[1])
    print(database1[2])   
    data=[]
    data.append(database1[1])
    data.append(database1[2])
    save(database1[0],chunks,data)
     
    print ("%i chunks received 2, %i bytes" % (chunks, total))
    #pipe.send(b"OK")

# The main task is just the same as in the first model.
# .skip

def main():

    # Start child threads
    ctx = zmq.Context()
    client_thread1(ctx)
    #b = ctx.socket(zmq.PAIR)
    #b.linger = 0
    #b.hwm = 1
    #iface = "inproc://%s" % binascii.hexlify(b'RA\xe3\xa7e\x0e:\xc3')
    
    #b.connect(iface)
    #CreateDatabase()
    #client = Thread(target=client_thread1, args=(ctx, b))
    #client.start()
   
    #num,d1,d2=getChunks(testdata)
    #print(num)
    #print(d1)
    #print(d2)
    
    
    
    # loop until client tells us it's done
    #del b
    #ctx.term()

if __name__ == '__main__':
    main()

