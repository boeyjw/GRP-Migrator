import threading
import re
import os
from sys import getsizeof
from json import dump
from queue import Queue
from connection import Connection

MAXDOCSIZE = 20000
lsjson = Queue(1000)
lstaxId = []

class Accession(threading.Thread):
    def __init__(self, table):
        threading.Thread.__init__(self)
        self.__conn = Connection()
        self.__cnx = self.__conn.getconn()
        self.__cursor = self.__cnx.cursor()
        self.__table = table
        self.acc = ('SELECT nne.accession, nne.`accession.version` AS version, nne.gi '
                    'FROM ncbi_{} nne INNER JOIN ncbi_nodes nn USING (tax_id) '
                    'WHERE nne.tax_id = {} AND nn.tax_id = {} {};')

    def run(self):
        for ids in lstaxId:
            idd = int(re.findall('\d+', str(ids))[0])
            self.__cursor.execute(self.acc.format(self.__table, idd, idd, ''))
            try:
                maxdoclength = self.__cursor.fetchmany(size=MAXDOCSIZE)
                while maxdoclength:
                    iddict = {
                        'taxId': idd,
                        self.__table: [
                            {
                                'accession': str(accession),
                                'version': str(version),
                                'gi': int(gi)
                            }
                            for (accession, version, gi) in maxdoclength
                        ]
                    }
                    if iddict[self.__table]:
                        lsjson.put(iddict)
                    maxdoclength = self.__cursor.fetchmany(size=MAXDOCSIZE)
                print('Normal: ' + str(idd) + '\t' + str(lsjson.qsize()))
            except MemoryError:
                limit = MAXDOCSIZE
                offset = 0
                while True:
                    self.__cursor.execute(self.acc.format(self.__table, idd, idd, 'limit ' + str(offset) + ',' + str(limit)))
                    if not self.__cursor:
                        break
                    else:
                        iddict = {
                            'taxId': idd,
                            self.__table: [
                                {
                                    'accession': str(accession),
                                    'version': str(version),
                                    'gi': int(gi)
                                }
                                for (accession, version, gi) in self.__cursor
                            ]
                        }
                        if iddict[self.__table]:
                            lsjson.put(iddict)
                        limit += MAXDOCSIZE
                        offset += limit
                print('Memory error: ' + str(idd) + '\t' + str(lsjson.qsize()))
        self.closeconn()

    def closeconn(self):
        self.__cursor.close()
        self.__conn.closeconn()

def makeidlist():
    conn = Connection()
    cnx = conn.getconn()
    cursor = cnx.cursor()
    cursor.execute('SELECT DISTINCT tax_id '
                   'FROM ncbi_nodes INNER JOIN gbif_ncbi_junction USING (tax_id) '
                   'ORDER BY tax_id;')
    for taxId in cursor:
        lstaxId.append(taxId)
    cursor.close()
    conn.closeconn()

def worker():
    while True:
        tmp = lsjson.get()
        print("[WRITE] " + str(tmp['taxId']))
        dump(tmp, wfile)
        checkalive = [x.is_alive() for x in threads]
        if any(True for x in checkalive):
            wfile.write(',')

makeidlist()
wfile = open('ncbi_acc.json', mode='w', encoding='utf-8')
wfile.write('[')
tables = ['nucl_gb', 'nucl_gss', 'nucl_wgs', 'nucl_est', 'prot']
threads = [Accession(t) for t in tables]
twriter = threading.Thread(target=worker)

for th in threads:
    th.start()
twriter.start()

for th in threads:
    th.join()

wfile.write(']')
print("File size: " + os.stat(wfile).st_size)
wfile.close()
twriter.join()
print('Completed')
