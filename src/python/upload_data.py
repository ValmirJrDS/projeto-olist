from importlib.resources import path
import os
import pandas as pd
import sqlalchemy


#os.path.abspath(__file__)

str_connection = 'sqlite:////{path}'

# Os endereços de nosso projeto e sub pastas
base_dir = os.path.dirname(os.path.dirname( os.path.dirname( os.path.abspath(__file__) ) ) )
data_dir = os.path.join( base_dir, 'data' )

# Encontrando os arquivos de dados
files_names = [ i for i in os.listdir(data_dir) if i.endswith('.csv')]

#Abrindo uma conexão com o banco
str_connection = str_connection.format( path = os.path.join(data_dir, 'olist.db'))
connection = sqlalchemy.create_engine(str_connection )

for i in files_names:
    print(i)
    df_tmp = pd.read_csv(os.path.join(data_dir, i))

    #melhoramento nas tabelas - criar uma variavel que faça: incluir tb_ na fr+ente do nome, variable "i" ira percorrer o diretorio
    #'data', dos nomes retirar "olist_" e subst por vazio, retirar "_dataset" e subst por vazio
    table_name = "tb_" + i.strip(".csv").replace("olist_", "").replace("_dataset", "")

    #Executar e criar um banco sql sera preciso a conexão e variable for the rules 
    df_tmp.to_sql( table_name, 
                   connection,
                   if_exists='replace',
                   index=False)
    


