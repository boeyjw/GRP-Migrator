import threading
import re
import os
import sys
import time
from json import dump
from queue import Queue
from connection import Connection

MAXDOCSIZE = 1000000
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
        """Queries and store json results into sync queue"""
        for ids in lstaxId:
            idd = int(re.findall('\d+', str(ids))[0])
            try:
                self.__cursor.execute(self.acc.format(self.__table, idd, idd, ''))
                maxdoclength = self.__cursor.fetchmany(size=MAXDOCSIZE)
                if maxdoclength:
                    print('[SIZE] ' + self.__table + "\t" + str(idd) + '\t' + str(sys.getsizeof(maxdoclength)))
                while maxdoclength:
                    iddict = {
                        'taxId': idd,
                        self.__table: [
                            {
                                'acc': str(accession),
                                'vers': str(version),
                                'gi': int(gi)
                            }
                            for (accession, version, gi) in maxdoclength
                        ]
                    }
                    if iddict[self.__table]:
                        lsjson.put(iddict)
                    maxdoclength = self.__cursor.fetchmany(size=MAXDOCSIZE)
                print('[Normal] ' + self.__table + "\t" + str(idd) + '\t' + str(lsjson.qsize()))
            except MemoryError:
                if self.__cursor:
                    self.__cursor.fetchall()
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
                        offset += limit
                print('[Memory error] ' + str(idd) + '\t' + str(lsjson.qsize()))
        self.closeconn()

    def closeconn(self):
        """Explicitly closes connection"""
        self.__cursor.close()
        self.__conn.closeconn()

def makeidlist():
    """Store entire list of tax_ids into shared Memory"""
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
    """Write into a single json file in json array format"""
    wfile.write('[')
    while True:
        tmp = lsjson.get()
        print("[WRITE] " + str(tmp['taxId']))
        dump(tmp, wfile)
        checkalive = [x.is_alive() for x in threads]
        if any(True for x in checkalive):
            wfile.write(',')
    wfile.write(']')

if __name__ == '__main__':
    makeidlist()
    wfile = open('ncbi_acc.json', mode='w', buffering=16*1024, encoding='utf-8')
    tables = ['nucl_gb', 'nucl_gss', 'nucl_wgs', 'nucl_est', 'prot']
    threads = [Accession(t) for t in tables]
    twriter = threading.Thread(target=worker)

    for th in threads:
        th.daemon = True
        th.start()
    twriter.daemon = True
    twriter.start()

    for th in threads:
        th.join(1000)
    twriter.join(1000)

    print("File size: " + str(os.stat('ncbi_acc.json').st_size))
    wfile.close()
    print('Completed')
