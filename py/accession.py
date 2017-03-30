import threading
import re
from json import dump
from queue import Queue
from sys import getsizeof
from connection import Connection

lsjson = Queue(10000)
lstaxId = []

class Accession(threading.Thread):
    def __init__(self, table):
        threading.Thread.__init__(self)
        self.__conn = Connection()
        self.__cnx = self.__conn.getconn()
        self.__cursor = self.__cnx.cursor(buffered=True)
        self.__table = table
        self.acc = ('SELECT nne.accession, nne.`accession.version` AS version, nne.gi '
                    'FROM ncbi_{} nne INNER JOIN ncbi_nodes nn USING (tax_id) '
                    'WHERE nne.tax_id = {} AND nn.tax_id = {}')

    def run(self):
        for ids in lstaxId:
            idd = int(re.findall('\d+', str(ids))[0])
            self.__cursor.execute(self.acc.format(self.__table, idd, idd))
            print(getsizeof(self.__cursor))
            try:
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
            except MemoryError:
                pass
            if iddict[self.__table]:
                lsjson.put(iddict)
            """print(str(idd) + '\t' + str(lsjson.qsize()))"""
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
        dump(lsjson.get(), wfile)
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

for t in threads:
    t.join()
twriter.join()
wfile.write(']')
wfile.close()
