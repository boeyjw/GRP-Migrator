import io
from connection import Connection

class Junction:
    """GBIF NCBI junction"""
    def __init__(self):
        self.__conn = Connection()
        self.__cnx = self.__conn.getconn()
        self.__cursor = self.__cnx.cursor(buffered=True)
        self.gquery = ('SELECT gt.taxonID, nn.tax_id '
                       'FROM {} gt INNER JOIN {} nn ON gt.{} = nn.{} '
                       'WHERE nn.{} = "{}";')
        self.uqquery = ('SELECT gt.taxonID, nn.tax_id '
                        'FROM {} gt INNER JOIN ncbi_names nn ON gt.{} = nn.unique_name '
                        'WHERE nn.unique_name LIKE "%<{}>";')
        self.__cquery = ('CREATE TABLE IF NOT EXISTS gbif_ncbi_junction ('
                         'taxonID INT(10) UNSIGNED NOT NULL,'
                         'tax_id MEDIUMINT(11) UNSIGNED NOT NULL);')

    def closeconn(self):
        self.__cursor.close()
        self.__conn.closeconn()

    def __rf(self):
        """Read all lines in file input"""
        with open('D:\\My Documents Placement\\Eclipse Java workspace\\GBIF-sql-to-json\\py\\cvn_nuq.comm', mode='r') as rfile:
            names = [lines.strip('\n') for lines in rfile.readlines()]
        return names

    def __selectstmt(self, gt, nn, gton, nnon, nnparam):
        self.__cursor.execute(self.gquery.format(gt, nn, gton, nnon, nnon, nnparam))
        list = [(taxonID, tax_id) for (taxonID, tax_id) in self.__cursor]
        return list
    
    def __uniqsel(self, gt, gton, nnparam):
        self.__cursor.execute(self.uqquery.format(gt, gton, nnparam))
        list = [(taxonID, tax_id) for (taxonID, tax_id) in self.__cursor]
        return list

    def runjunction(self):
        gt = 'gbif_taxon'
        gv = 'gbif_vernacularname'
        gtcn = 'canonicalName'
        gtvn = 'vernacularName'
        nn = 'ncbi_names'
        nne = 'name_txt'
        self.__cursor.execute(self.__cquery)
        names = self.__rf()
        with open('gbif_ncbi_junction.csv', mode='w', buffering=1, encoding='utf-8') as wfile:
            for lines in names:
                res = self.__selectstmt(gt, nn, gtcn, nne, lines)
                if not res:
                    res = self.__uniqsel(gt, gtcn, lines)
                    if not res:
                        res = self.__selectstmt(gv, nn, gtvn, nne, lines)
                        if not res:
                            res = self.__uniqsel(gv, gtvn, lines)
                else:
                    for (taxonID, tax_id) in res:
                        print((taxonID, tax_id))
                        wfile.write(', '.join(map(str, (taxonID, tax_id))) + '\n')

obj = Junction()
obj.runjunction()
obj.closeconn()
