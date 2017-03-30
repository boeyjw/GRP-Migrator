import io
import os
import threading
import re
from json import dumps
from sys import getsizeof
from connection import Connection

class Accession(threading.Thread):
    def __init__(self, table):
        threading.Thread.__init__(self)
        self.__conn = Connection()
        self.__cnx = self.__conn.getconn()
        self.__cursor = self.__cnx.cursor(buffered=True)
        self.__cursor.execute('SELECT DISTINCT tax_id '
                              'FROM ncbi_nodes INNER JOIN gbif_ncbi_junction USING (tax_id) '
                              'ORDER BY tax_id;')
        self.__lstaxId = [tax_id for tax_id in self.__cursor]
        self.__table = table
        self.__filecounter = 0
        self.acc = ('SELECT nne.accession, nne.`accession.version` AS version, nne.gi '
                    'FROM ncbi_{} nne INNER JOIN ncbi_nodes nn USING (tax_id) '
                    'WHERE nne.tax_id = {} AND nn.tax_id = {}')

    def run(self):
        lsjson = []
        for ids in self.__lstaxId:
            idd = int(re.findall('\d+', str(ids))[0])
            """print(self.__table + "\t" + str(idd))"""
            self.__cursor.execute(self.acc.format(self.__table, idd, idd))
            iddict = {
                'taxId': idd,
                self.__table: [
                    {
                        'accession': accession,
                        'version': version,
                        'gi': gi
                    }
                    for (accession, version, gi) in self.__cursor
                ]
            }
            lsjson.append(iddict)
            print(getsizeof(lsjson))
            if getsizeof(lsjson) >= 512*1024:
                self.writeToFile(lsjson)
                del lsjson[:]
        if lsjson:
            self.writeToFile(lsjson)
            del lsjson[:]
        self.closeconn()

    def writeToFile(self, lsjson):
        with open('ncbi_' + self.__table + '_' + self.__filecounter, mode='w', encoding='utf-8') as wfile:
            wfile.write(dumps(lsjson))
        print((os.stat('ncbi_' + self.__table + '_' + self.__filecounter).st_size / 1024))
        self.__filecounter += 1

    def closeconn(self):
        self.__cursor.close()
        self.__conn.closeconn()

tables = ['nucl_gb', 'nucl_gss', 'nucl_wgs', 'nucl_est', 'prot']
threads = []
for t in tables:
    threads.append(Accession(t))

for th in threads:
    th.start()

for t in threads:
    t.join()
