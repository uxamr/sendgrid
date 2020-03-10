import boto3
import creds
from io import StringIO
from sqlalchemy import create_engine
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange

class Redshift() :
    def __init__(self):
        """Motor para la conexion con Redshift"""
        self.engine = create_engine("postgresql+psycopg2://{user}:{contr}@{host}:{port}/{base}".format( user = creds.redshift["user"], 
                                                                                                    contr = creds.redshift["password"],
                                                                                                    port = creds.redshift["port"],
                                                                                                    base = creds.redshift["database"], 
                                                                                                    host = creds.redshift["host"] ), 
                                    connect_args={'options': '-csearch_path={schema}'.format( schema = creds.redshift["schema"] )}, echo = False)

class fechas(): 
    def __init__(self, inicio = "2018-12-01", tipo = "months"):
        """
        inicio: fecha en formato %Y-%m-%d. Este valor se tomara como referencia para el calculo del intervalo. DEFAULT = 2018-12-01
        Solo para extracciones completas
        """
        if tipo == "months": 
            delta = relativedelta(months =+ 1)
        if tipo == "weeks": 
            delta = relativedelta(weeks =+1)

        self.tipo = tipo
        date1 = datetime.strptime(str(inicio), "%Y-%m-%d")
        d = date1
        salida = []
        while d<=datetime.strptime(str(date.today()), "%Y-%m-%d"): 
            salida.append(d.strftime("%Y-%m-%d"))
            d += delta
        self.fechas = salida
        self.ends = self.__last_day(self.fechas)
        self.fechas = [(start, end) for start, end in zip(self.fechas, self.ends)]
    
    def __last_day(self, lista):
        end_list = []
        if self.tipo == "months": 
            for i in lista: 
                dia = monthrange(int(i[:4]),int(i[5:7]))
                end_list.append("{}-{}-{}".format(i[:4],i[5:7],dia[1]))
            return end_list
        if self.tipo == "weeks":
            delta = relativedelta(weeks=+1)
            for i in lista: 
                end_list.append(str(datetime.strptime(str(i), "%Y-%m-%d") + delta)[:10])
            return end_list

class Semana(): 
    def __init__(self, dias = 7): 
        """
        Para extraccion semanal en cronjob. El valor por default de dias que se tomara es de 7  
        """
        today = date.today()
        dias = timedelta(days= dias)
        self.fechas = (str(today-dias), str(date.today()))

class New_Columns() :
    def __init__(self, tabla, eng, name) :
        """
        Inserta las columnas nuevas del dataframe que no estén en la base de datos.
        tabla: Dataframe que se quiere insertar
        eng: motor de conexion
        name: nombre de la tabla en la db
        """
        types = {"int": "bigint",
                 "int64": "bigint",
                 "bool": "boolean",
                 "float64": "double precision",
                 "object": "character varying (65535)"}    
        cols = tabla.columns
        with eng.connect() as conn :
            try :
                q = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}'".format(creds.redshift["schema"],name))
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
    def __init__(self, dat, name, engine, pkey = "", fkey = [], ref = [], d_type = "character varying (65535)") :
        """
        Crea una nueva tabla en la base de datos.
        name: nombre de la tabla en la db
        engine: motor de conexion
        pkey: llave primaria
        fkey: lista de llaves foraneas
        ref: lista de diccionarios que enlazan las llaves primarias con su referencia. La lista debe ser del mismo tamaño que fkey y deben seguir el mismo orden
            [{"table":"nombre_de_la_tabla", "key":"llave_de_la_referencia"}]
        """
        with engine.connect() as conn :
            conn.execute("DROP TABLE IF EXISTS {} CASCADE".format(name))
            if pkey != "" :
            	conn.execute("CREATE TABLE {t_name}({index} {dtype} PRIMARY KEY)".format(t_name=name, index = pkey, dtype = d_type))
            elif pkey == "" :
                conn.execute("CREATE TABLE {t_name}({index} {dtype} PRIMARY KEY)".format(t_name=name, index = "index", dtype = d_type))
            if ref != [] and len(fkey) == len(ref):
            	for i in range(len(fkey)) :
                    conn.execute("ALTER TABLE {t_name} ADD COLUMN {col_name} character varying (65535)".format(t_name=name, col_name=fkey[i]))
                    conn.execute("ALTER TABLE {t_name} ADD FOREIGN KEY ({index}) REFERENCES  {ref_tab} ({ref_index})".format(t_name=name, index=fkey[i], ref_tab=ref[i]["table"], ref_index=ref[i]["key"]))
#            		conn.execute("CREATE TABLE {t_name}({index} bigint FOREIGN KEY ({index}) REFERENCES {ref_tab} ({ref_index}))".format(t_name=name, index=fkey[i], ref_tab=ref[i]["table"], ref_ind=ref[i]["key"]))
            if pkey == "":
                conn.execute("ALTER TABLE {} DROP COLUMN index".format(name))
        New_Columns(tabla = dat, eng = engine, name = name)

class Upload_Redshift() :
    def __init__(self, df, name_table, engine) :
        """
        Sube los datos de un dataframe a una tabla en una instancia de Redshift, empleando un bucket de S3 como intermediario
        df: dataframe a insertar
        name_table: nombre de la tabla en la db en la que se quieren insertar los datos
        engine: motor de conexion
        """
        session = boto3.Session(
            aws_access_key_id = creds.aws["key"],
            aws_secret_access_key = creds.aws["secret_key"])

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index = False)
        s3_resource = session.resource('s3')
        s3_resource.Object(creds.s3["bucket"], creds.s3["folder"]+"/"+str(name_table)+".csv").put(Body=csv_buffer.getvalue())

        cols = df.columns
        with engine.connect() as conn :
            conn.execute("COPY {schema}.{table} ({cols}) FROM '{s3}' WITH CREDENTIALS 'aws_access_key_id={keyid};aws_secret_access_key={secretid}' CSV IGNOREHEADER 1 EMPTYASNULL;commit;".format(schema = creds.redshift["schema"], 
                                                                                                                                                                                                    table = name_table,
                                                                                                                                                                                                    cols = ', '.join(cols[j] for j in range( len(cols) ) ),
                                                                                                                                                                                                    s3='s3://{}/{}/{}'.format(creds.s3["bucket"], creds.s3["folder"],name_table+".csv"),
                                                                                                                                                                                                    keyid = creds.aws["key"],
                                                                                                                                                                                                    secretid= creds.aws["secret_key"]))
