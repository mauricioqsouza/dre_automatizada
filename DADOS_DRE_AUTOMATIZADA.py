import gspread
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from datetime import date
import requests
from config import sheet1, sheet2, DATABASE_URL, apiKey, tabela1, tabela2
import gc

scopes = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

json_file = "dre_dd.json"

def login():
    credentials = service_account.Credentials.from_service_account_file(json_file)
    scoped_credentials = credentials.with_scopes(scopes)
    gc = gspread.authorize(scoped_credentials)
    return gc
gc = login()
print("LOGIN da API do Sheets, Realizado!")
# Horário da atualização:
horario = datetime.now()
print(horario)

# Conexion BQ:
SCOPES = ['https://www.googleapis.com/auth/cloud-platform']

credentials = service_account.Credentials.from_service_account_file("dre_dd.json")
client_bigquery = bigquery.Client.from_service_account_json("dre_dd.json")

pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = "dre"

#Abre a planilha
sheet = gc.open_by_key(sheet1)
#Para selecionar a planilha pelo nome, use o código abaixo:
#sheet = gc.open('NOME')

#Selecionando a WORKSHEET:
#worksheet_list = sheet.worksheets()
#print(worksheet_list)
#worksheet = sheet.get_worksheet(2)
worksheet = sheet.worksheet("BD_IMPORT")
worksheet.resize(rows=5)

dados_co = pd.read_csv(DATABASE_URL, sep= ';', encoding_errors='ignore', on_bad_lines='skip')
dados_co = pd.DataFrame(dados_co)
dados_co.info()
dados_co = dados_co.drop(columns=dados_co.columns[7])
agrupamento = dados_co[['TIPO', 'CC', 'CC_DESC', 'CO', 'CO_DESC', 'ANOMES', 'VALOR']]

#Filtrando os últimos 3 Anos:
agrupamento['ANOMES'] = pd.to_numeric(agrupamento['ANOMES'], errors='coerce')
filtrado = agrupamento.loc[(agrupamento['ANOMES']>=202100) & (agrupamento['CC']!=141480015)]

#Evitando erro de Slice:
consolidado = filtrado.copy()

#Substituindo Valores:
consolidado.loc[(consolidado['CC']==151540001) | (consolidado['CC']==151540002) | (consolidado['CC']==150540001), 'CO_DESC'] = 'RECURSOS HUMANOS - RhBA'

#Selecionando Colunas e substituindo valores:
#agrupamento[['coluna']] = agrupamento[['coluna']].replace(0,'')

print(consolidado.head(5))

worksheet.update([consolidado.columns.values.tolist()] + consolidado.values.tolist())

##Enviar para o BQ:
print("Enviando o DataFrame consolidado para o BigQuery:")


job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
job = client_bigquery.load_table_from_dataframe(consolidado, tabela1,
                                                job_config=job_config)
job.result()

print("Tabela consolidada enviada!")

# Limpar todas as variáveis locais
for var in list(locals().keys()):
    del locals()[var]
gc.collect()

print("Dados por CO: importados e inseridos na DRE!")

### Employee na Planilha:

#Abre a planilha
sheet = gc.open_by_key(sheet2)
#Selecionando a WORKSHEET:
employee = sheet.worksheet("Employee Automático")

#API do Monday:

apiUrl = "https://api.monday.com/v2"
headers = {"Authorization" : os.getenv(apiKey)}

#Query GraphQl:

query = '{boards(ids: 1912587) {items {group{title} name column_values{title text}}}}'
data = {'query' : query}

#query2 = '{boards(ids: 1912587) {group{title} columns{settings_str}}}'
#data2 = {'query' : query2}

#make request
resposta = requests.post(url=apiUrl, json=data, headers=headers)
print(resposta.json())
info = resposta.json()

#resposta2 = requests.post(url=apiUrl, json=data2, headers=headers)
#print(resposta2.json())
#info = resposta2.json()

#Tratando o Json:

list_of_dict = []

for x in info['data']['boards'][0]['items']:
    df_dict = {}
    df_dict['Group'] = x['group']['title']

    df_dict['Name'] = x['name']

    for y in x['column_values']:
        df_dict[y['title']] = y['text']

    list_of_dict.append(df_dict)

df = pd.DataFrame(list_of_dict)

print("Consulta Realizada!")

#Alterar colunas para float64 e substituir os valores nulos:

df['Salario Bruto'] = pd.to_numeric(df['Salario Bruto'], errors='coerce')
df['Alimentacao'] = pd.to_numeric(df['Alimentacao'], errors='coerce')
df['Ticket Alimentacao'] = pd.to_numeric(df['Ticket Alimentacao'], errors='coerce')
df['Gasolina'] = pd.to_numeric(df['Gasolina'], errors='coerce')
df['Transporte'] = pd.to_numeric(df['Transporte'], errors='coerce')
df['Plano Saude'] = pd.to_numeric(df['Plano Saude'], errors='coerce')
df['Seguro de Vida'] = pd.to_numeric(df['Seguro de Vida'], errors='coerce')
df['Auxilio Creche'] = pd.to_numeric(df['Auxilio Creche'], errors='coerce')
df['Idioma'] = pd.to_numeric(df['Idioma'], errors='coerce')
df['Internet'] = pd.to_numeric(df['Internet'], errors='coerce')
df['Beneficio de Capacitação'] = pd.to_numeric(df['Beneficio de Capacitação'], errors='coerce')
df['Meses p Rescisao'] = pd.to_numeric(df['Meses p Rescisao'], errors='coerce')
df = df.fillna(0)

#Calculando colunas:
df['Total BENEFICIOS'] = df['Alimentacao']+df['Ticket Alimentacao']+df['Gasolina']+df['Transporte']+df['Plano Saude']+df['Seguro de Vida']+df['Auxilio Creche']+df['Idioma']+df['Internet']+df['Beneficio de Capacitação']
df['Enc-INSS'] = df['Salario Bruto']*0.27
df['Enc-FGTS'] = df['Salario Bruto']*(8/100)
df['Enc-PIS'] = df['Salario Bruto']*0.01
df['Total ENCARGOS'] = df['Enc-INSS']+df['Enc-FGTS']+df['Enc-PIS']
df['Prov-13Sal'] = df['Salario Bruto']*1
df['Prov-INSS-13ro'] = df['Prov-13Sal']*0.27
df['Prov-FGTS-13ro'] = df['Prov-13Sal']*0.08
df['Prov-PIS-13ro'] = df['Prov-13Sal']*0.01
df['Prov-Adic-Ferias'] = df['Salario Bruto']*(1/3)
df['Prov-INSS-Ferias'] = df['Prov-Adic-Ferias']*0.27
df['Prov-FGTS-Ferias'] = df['Prov-Adic-Ferias']*0.08
df['Prov-PIS-Ferias'] = df['Prov-Adic-Ferias']*0.01
df['Total PROVISOES'] = (df['Prov-13Sal']+df['Prov-INSS-13ro']+df['Prov-FGTS-13ro']+df['Prov-PIS-13ro']+df['Prov-Adic-Ferias']+df['Prov-INSS-Ferias']+df['Prov-FGTS-Ferias']+df['Prov-PIS-Ferias'])/df['Meses p Rescisao']
df['Resc-Aviso-Previo-Indenizado'] = df['Salario Bruto']*1
df['Resc-13ro-Indeniz'] = df['Resc-Aviso-Previo-Indenizado']*(1/12)
df['Resc--Ferias-Indeniz'] = ((df['Resc-Aviso-Previo-Indenizado']*1.33)/12)/12
df['Resc-INSS-Rescisao'] = df['Resc-Aviso-Previo-Indenizado']*0.27
df['Resc-PIS Rescisão'] = df['Resc-Aviso-Previo-Indenizado']*0.01
df['Resc-FGTS-Rescisao'] = df['Resc-Aviso-Previo-Indenizado']*0.08
df['Resc-Multa-FGTS'] = 0.5*(df['Enc-FGTS']*df['Meses p Rescisao']+df['Prov-FGTS-13ro']+df['Prov-FGTS-Ferias'])
df['Total RESCISOES'] = (df['Resc-Aviso-Previo-Indenizado']+df['Resc-13ro-Indeniz']+df['Resc--Ferias-Indeniz']+df['Resc-INSS-Rescisao']+df['Resc-PIS Rescisão']+df['Resc-FGTS-Rescisao']+df['Resc-Multa-FGTS'])/df['Meses p Rescisao']
df['Custo Mensal'] = df['Salario Bruto']+df['Total BENEFICIOS']+df['Total ENCARGOS']+df['Total PROVISOES']+df['Total RESCISOES']
df['Custo Anual'] = df['Meses p Rescisao'] * df['Custo Mensal']

#Corrifindo o Erro: Out of range float values are not JSON compliant
df['Custo Mensal'] = round(df['Custo Mensal'],2)

df['Custo Mensal'] = pd.to_numeric(df['Custo Mensal'], errors='coerce')
df = df.fillna(0)

#df.info()
#Selecionando colunas:

df = df[['Name', 'Matr', 'Area', 'Site', 'Tipo MOD', 'Função', 'Salario Bruto', 'Custo Mensal']]

print(df.head())
df.info()

#df.to_csv('teste.csv')
#Redimensionando a WORKSHEET:
nlinhas = df.shape[0]
employee.resize(rows=nlinhas+1)
print(f"Redimensionando a planilha para {nlinhas+1} linhas...")

#Colar o df no G Sheets:
employee.update([df.columns.values.tolist()] + df.values.tolist())

print("Atualizado com Sucesso!")

##Enviar para o BQ:
print("Enviando o DataFrame df para o BigQuery:")


job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
job = client_bigquery.load_table_from_dataframe(df, tabela2,
                                                job_config=job_config)
job.result()

print("Tabela df enviada!")

# Limpar todas as variáveis locais
for var in list(locals().keys()):
    del locals()[var]
gc.collect()