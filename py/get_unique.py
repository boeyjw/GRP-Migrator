import io
import mysql.connector

cnx = mysql.connector.connect(user='root', password='', host='127.0.0.1', database='merge')
cursor = cnx.cursor(buffered=True)

query = "SELECT unique_name FROM ncbi_names WHERE unique_name IS NOT NULL AND (name_class = 'scientificName' OR name_class='in-part' OR name_class='common name' OR name_class='genbank commong name' OR name_class='synonym' or name_class='anamorph')"

cursor.execute(query)

with open("ncbi_uq.txt", "w", encoding="utf-8") as file:
    for unique_name in cursor:
        file.write(''.join(unique_name).split("<", 1)[1].split(">")[0] + "\n")

cursor.close()
cnx.close()
