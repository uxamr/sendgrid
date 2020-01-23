import boto3
import pandas as pd
import os
import time


#Regresa las columnas nuevas del dataframe que se quiere insertar que no estan en la bd y las inserta
class New_Columns() :
    def __init__(self, tabla, eng, name) :
        types = {"int": "bigint",
                 "int64": "bigint",
                 "bool": "boolean",
                 "float64": "double precision",
                 "object": "character varying (65535)"}    
        cols = tabla.columns
        with eng.connect() as conn :
            try :
                q = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '{}'".format(name))
                cols_db = []
                for i in q :
                    cols_db.append(i[0])
    #            print("Columnas en la db ", cols_db)
                if len(cols_db) == 0:
                    self.cols = cols_db
                else :
                    self.cols = list(set(cols) - set(cols_db))
            except :
                self.cols = []
            #Agrega las columnas nuevas que se tengan que agregar
            if len(self.cols) != 0 :
                print("Columnas nuevas: ", self.cols)
                for j in self.cols :
                    #Muestra el tipo de dato de las columnas nuevas
#                    print(j, str(tabla[j].dtypes))
                    conn.execute("ALTER TABLE {} ADD COLUMN {} {}".format(name, str(j),types[ str(tabla[j].dtypes) ]))
                    
class Initialize() :
    def __init__(self, name, engine, column, d_type) :
        with engine.connect() as conn :
            conn.execute("DROP TABLE IF EXISTS {}".format(name))
            conn.execute("CREATE TABLE {}({} {})".format(name, column, d_type))


class Upload_S3() :
    def __init__(self, name) :
        session = boto3.Session(
            aws_access_key_id = os.environ["AWS_KEY"],
            aws_secret_access_key = os.environ["AWS_SECRET_KEY"])
    
        s3 = session.client('s3')
        
        t0 = time.time()
        for i in name :
            with open(i, "rb") as f:
                s3.upload_fileobj(f, os.environ["S3_Bucket"], i)
                
        t1 = time.time()
        
        print("Tiempo de subida a S3: ", t1-t0)
        
        
class Copy_Redshift() :
    def __init__(self, name_csv, name_table, engine) :
        csv = pd.read_csv(name_csv)
        cols = csv.columns
        with engine.connect() as conn :
            conn.execute("COPY {schema}.{table} ({cols}) FROM '{s3}' WITH CREDENTIALS 'aws_access_key_id={keyid};aws_secret_access_key={secretid}' CSV IGNOREHEADER 1 EMPTYASNULL;commit;".format(schema = os.environ["REDSHIFT_SCHEMA"], 
                                                                                                                                                                                                    table = name_table,
                                                                                                                                                                                                    cols = ', '.join(cols[j] for j in range( len(cols) ) ),
                                                                                                                                                                                                    s3='s3://{}/{}'.format(os.environ["S3_Bucket"],name_csv),
                                                                                                                                                                                                    keyid= os.environ["AWS_KEY"],
                                                                                                                                                                                                    secretid= os.environ["AWS_SECRET_KEY"]))
            
                                                        
    
    
    
    
