import logging
import os
import json
import time
import shutil


from tse import rdv_dump, rdv_resumo
from tse.rdv_resumo import *
from tse import bu_dump
from tse.bu_dump import *

import pandas as pd
import numpy as np

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

#Logger
logger = logging.getLogger('process_data')
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

#CONSTANS
BASE_PATH = "D:\\Workspace\\eleicoes2022-base"
DOWNLOADS_PATH = BASE_PATH + "\\downloads"
PATH_TO_BASES =  BASE_PATH +  '\\bases_processadas'

#Load already downloaded files?
CARREGAR_PROCESSADAS = False


## Base 1
### From UF to Secao, with filepaths
def base_secao_with_files(downloads_path, logger):
    CICLO = ""
    PLEITO = ""
    ELEICAO = ""
    UF = ""
    UF_DS = ""
    MUNICIPIO = ""
    MUNICIPIO_NM = ""
    ZONA = ""
    SECAO = ""

    def get_secao_obj(ciclo, pleito, eleicao, uf, uf_ds, municipio, municipio_ds, zona, secao):
        return {"ciclo":ciclo, "pleito":pleito, "eleicao":eleicao, "uf":uf, "uf_ds":uf_ds, "municipio":municipio, "municipio_ds":municipio_ds, "zona":zona, "secao":secao, "rdv":None, "imgbu": None, "logjez": None, "vscmr": None, "bu": None}
    SECAO_OBJ = {}
    lista = []

    F1_FILE_PATH = "{}\\{}".format(downloads_path, "ele-c.json")
    if (not os.path.isfile(F1_FILE_PATH)):
        raise Exception("F1 file does not exists in {}".format(F1_FILE_PATH)) 


    with open(F1_FILE_PATH, 'r', encoding='utf-8') as f_json:
        json_obj = json.load(f_json)

    CICLO = json_obj['c']
    PLEITOS = json_obj['pl']
    for pleito in PLEITOS:
        PLEITO = pleito['cd']
        logger.info("Pleito %s",PLEITO)
        for eleicao in pleito['e']:
            ELEICAO = eleicao['cd']
            logger.info("Eleição %s",ELEICAO)

            F2_CONFIG_PATH = "{}\\{}\\{}\\config".format(downloads_path, CICLO, ELEICAO)
            F2_FILE_PATH = "{}/{}".format(F2_CONFIG_PATH, 'mun-e{}-cm.json'.format(ELEICAO.zfill(6)))
            if (not os.path.isfile(F2_FILE_PATH)):
                continue
            
            with open(F2_FILE_PATH, 'r', encoding='utf-8') as f_json:
                F3_CONFIG_PATH = "{}\\{}\\arquivo-urna\\{}\\config".format(downloads_path, CICLO, PLEITO)
                municipios_json = json.load(f_json)
                
            for uf in municipios_json['abr']:
                UF = uf['cd'].lower()
                UF_DS = uf['ds']
                logger.info("Estado %s - %s",UF_DS, UF)
            
                F3_CONFIG_PATH_UF = "{}\\{}".format(F3_CONFIG_PATH,UF)
                F3_FILE = '{}/{}'.format(F3_CONFIG_PATH_UF, '{}-p{}-cs.json'.format(UF, PLEITO.zfill(6)))
                with open(F3_FILE, 'r', encoding='utf-8') as esf_json:
                    estado_json = json.load(esf_json)

                for cidade in estado_json['abr'][0]['mu']:
                    MUNICIPIO = cidade['cd']
                    MUNICIPIO_NM = cidade['nm']

                    for zona in cidade['zon']:
                        ZONA = zona['cd']
                        for secao in zona['sec']:

                            SECAO = secao['ns']

                            SECAO_OBJ = get_secao_obj(CICLO, PLEITO, ELEICAO, UF, UF_DS, MUNICIPIO, MUNICIPIO_NM, ZONA, SECAO)
                            
                            DADOS_URNA_PATH = "{}\\{}\\arquivo-urna\\{}\\dados\\{}\\{}\\{}\\{}".format(downloads_path, CICLO, PLEITO, UF, MUNICIPIO, ZONA.zfill(4), SECAO.zfill(4))
                            URNA_DADOS_FILENAME = 'p{}-{}-m{}-z{}-s{}-aux.json'.format(PLEITO.zfill(6), UF, MUNICIPIO.zfill(5), ZONA.zfill(4), SECAO.zfill(4))
                            with open('{}\\{}'.format(DADOS_URNA_PATH, URNA_DADOS_FILENAME), 'r', encoding='utf-8') as esf_json:
                                urna_files_json = json.load(esf_json)

                            urna_hash = urna_files_json['hashes'][0]['hash']
                            DADOS_URNA_HASH_PATH = '{}\\{}'.format(DADOS_URNA_PATH, urna_hash)
                            if(urna_files_json['hashes'][0]['st'] == "Sem arquivo"):
                                continue

                            for file in urna_files_json['hashes'][0]['nmarq']:
                                extension = file.split(".")[1]
                                filepath = '{}/{}'.format(DADOS_URNA_HASH_PATH, file)
                                SECAO_OBJ[extension] = filepath

                            lista.append(SECAO_OBJ)
    return pd.DataFrame(lista)

if (not CARREGAR_PROCESSADAS):
    b1_secoes_files = base_secao_with_files(DOWNLOADS_PATH, logger)
else:
    b1_secoes_files = pd.read_csv(PATH_TO_BASES + "\\"+"b1_secoes_files.csv")
    b1_secoes_files = b1_secoes_files.iloc[:, 1:]

if (not CARREGAR_PROCESSADAS):
    if not os.path.exists(PATH_TO_BASES):
        os.mkdir(PATH_TO_BASES)
    b1_secoes_files.to_csv(PATH_TO_BASES + "\\"+"b1_secoes_files.csv")


# Base 2
## Secao with attendance
asn1_path = './tse/spec/bu.asn1'
conv = asn1tools.compile_files(asn1_path)

def get_bu_data(bu_file):
    #logger.debug("%s", bu_file)
    if (bu_file is None or not os.path.exists(bu_file)):
        return pd.Series([np.nan, np.nan, np.nan])
    
    with open(bu_file, 'rb') as bu:
        envelope_encoded = bytearray(bu.read())

    envelope_decoded = conv.decode("EntidadeEnvelopeGenerico",envelope_encoded)
    bu_encoded = envelope_decoded["conteudo"]
    bu_decoded = conv.decode("EntidadeBoletimUrna", bu_encoded)
    eleitores_aptos = 0
    comparecimento = 0
    eleitores_faltosos = 0

    for resultadoVotacaoPorEleicao in bu_decoded['resultadosVotacaoPorEleicao']:
        eleitores_aptos = resultadoVotacaoPorEleicao['qtdEleitoresAptos']
        max_comparecimento = 0
        for resultadoVotacao in resultadoVotacaoPorEleicao['resultadosVotacao']:
            if (resultadoVotacao['qtdComparecimento'] > max_comparecimento):
                max_comparecimento = resultadoVotacao['qtdComparecimento']
        comparecimento = max_comparecimento
    
    eleitores_faltosos = eleitores_aptos - comparecimento

    return pd.Series([eleitores_aptos, comparecimento, eleitores_faltosos])


def get_bu_data_for_row(row):
    #logger.debug("%s %s-%s s:%s z%s",row['municipio'],row['municipio_ds'], row['uf'], row['zona'], row['secao'])
    file_path = row['bu'] if pd.notnull(row['bu']) else row['busa']

    return get_bu_data(file_path)

def get_bu_data_for_df(a_df):
    logger.info("Running get_bu_data_for_df for a dataframe of %s lines", len(a_df))
    #return a_df
    return a_df.apply(get_bu_data_for_row, axis=1)

if (not CARREGAR_PROCESSADAS):
    b2_secoes_attendance = b1_secoes_files.copy()

    df3 = get_bu_data_for_df(b2_secoes_attendance)
    b2_secoes_attendance[['eleitores_aptos', 'comparecimento', 'eleitores_faltosos']] = df3
    b2_secoes_attendance['pct_comparecimento'] = b2_secoes_attendance['comparecimento'] / b2_secoes_attendance['eleitores_aptos']
    del df3
else:
    b2_secoes_attendance = pd.read_csv(PATH_TO_BASES + "\\"+"b2_secoes_attendance.csv")
    b2_secoes_attendance = b2_secoes_attendance.iloc[:, 1:]

if (not CARREGAR_PROCESSADAS):
        if not os.path.exists(PATH_TO_BASES):
                os.mkdir(PATH_TO_BASES)

        b2_secoes_attendance.to_csv(PATH_TO_BASES + "\\"+"b2_secoes_attendance.csv")

# Base 3
## Secao with votting resume
def get_bu_data_deep(bu_file, ciclo, pleito, eleicao, uf,municipio, zona, secao):
    #logger.debug("%s", bu_file)
    if (bu_file is None or not os.path.exists(bu_file)):
        return pd.Series([np.nan, np.nan, np.nan])
    
    with open(bu_file, 'rb') as bu:
        envelope_encoded = bytearray(bu.read())

    envelope_decoded = conv.decode("EntidadeEnvelopeGenerico",envelope_encoded)
    bu_encoded = envelope_decoded["conteudo"]
    bu_decoded = conv.decode("EntidadeBoletimUrna", bu_encoded)

    lista = []

    for resultadoVotacaoPorEleicao in bu_decoded['resultadosVotacaoPorEleicao']:
        ELEICAO = resultadoVotacaoPorEleicao['idEleicao']
        for resultadoVotacao in resultadoVotacaoPorEleicao['resultadosVotacao']:
            for totaisVotosCargo in resultadoVotacao['totaisVotosCargo']:
                codigoCargo = totaisVotosCargo['codigoCargo'][1]
                for votosVotaveis in totaisVotosCargo['votosVotaveis']:
                    tipoVoto = votosVotaveis['tipoVoto']
                    quantidadeVotos = votosVotaveis['quantidadeVotos']

                    identificacaoVotavel = votosVotaveis['identificacaoVotavel'] if 'identificacaoVotavel' in votosVotaveis else None
                    identificacaoVotavelPartido = votosVotaveis['identificacaoVotavel']['partido'] if 'identificacaoVotavel' in votosVotaveis else None
                    identificacaoVotavelCodigo = votosVotaveis['identificacaoVotavel']['codigo'] if 'identificacaoVotavel' in votosVotaveis else None
                    
                    lista.append({"ciclo":ciclo, "pleito":pleito, "eleicao":eleicao, "uf":uf,  "municipio":municipio, "zona":zona, "secao":secao, 'codigoCargo':codigoCargo, 'tipoVoto': tipoVoto, 'quantidadeVotos': quantidadeVotos, 'identificacaoVotavelPartido': identificacaoVotavelPartido, 'identificacaoVotavelCodigo': identificacaoVotavelCodigo})  
    
    return lista

def get_bu_data_deep_for_row(row):
    file_path = row['bu'] if pd.notnull(row['bu']) else row['busa']
    return get_bu_data_deep(file_path, row['ciclo'], row['pleito'], row['eleicao'], row['uf'], row['municipio'], row['zona'], row['secao'])

def get_bu_data_deep_for_df(a_df):
    logger.info("Running get_bu_data_deep_for_df for a dataframe of %s lines", len(a_df))
    #return a_df
    return a_df.apply(get_bu_data_deep_for_row, axis=1)

def get_bu_data_deep_for_df(a_df):
    dfs = []
    for index, row in a_df.iterrows():
        dfs.append(pd.DataFrame(get_bu_data_deep_for_row(row)))
    return pd.concat(dfs)

if (not CARREGAR_PROCESSADAS):
    b3_secoes_votting_resume = get_bu_data_deep_for_df(b2_secoes_attendance)
else:
    b3_secoes_votting_resume = pd.read_csv(PATH_TO_BASES + "\\"+"b3_secoes_votting_resume.csv")
    b3_secoes_votting_resume = b3_secoes_votting_resume.iloc[:, 1:]

if (not CARREGAR_PROCESSADAS):
        if not os.path.exists(PATH_TO_BASES):
                os.mkdir(PATH_TO_BASES)

        b3_secoes_votting_resume.to_csv(PATH_TO_BASES + "\\"+"b3_secoes_votting_resume.csv")


# Base 4
## City, bolsonaro x lula
if (not CARREGAR_PROCESSADAS):
    #filto_votos_presidente = ((votos_df['codigoCargo'] == 'presidente') & (votos_df['uf'] == 'rr'))
    filto_votos_presidente = ((b3_secoes_votting_resume['codigoCargo'] == 'presidente'))
    b4_cidades_lula_x_bolsonaro = b3_secoes_votting_resume[filto_votos_presidente].groupby(['uf','municipio','identificacaoVotavelCodigo']).sum().reset_index()
    def classifica_lula_bolsonaro(row):
        candidato = 'outro'
        if(row['identificacaoVotavelCodigo'] == 13 ):
            candidato = 'lula'
        elif (row['identificacaoVotavelCodigo'] == 22 ):
            candidato = 'bolsonaro'
        return candidato

    b4_cidades_lula_x_bolsonaro['bolsonaro_lula'] = b4_cidades_lula_x_bolsonaro.apply(classifica_lula_bolsonaro, axis=1)
    b4_cidades_lula_x_bolsonaro = pd.pivot_table(b4_cidades_lula_x_bolsonaro, index=['uf','municipio'], columns=['bolsonaro_lula'], values='quantidadeVotos', aggfunc=np.sum).reset_index()
    b4_cidades_lula_x_bolsonaro.columns.name = ""
    b4_cidades_lula_x_bolsonaro.fillna(0, inplace=True)

    b4_cidades_lula_x_bolsonaro['total_presidente'] = b4_cidades_lula_x_bolsonaro['lula'] + b4_cidades_lula_x_bolsonaro['bolsonaro'] + b4_cidades_lula_x_bolsonaro['outro']
    b4_cidades_lula_x_bolsonaro['bolsonaro_pct'] = b4_cidades_lula_x_bolsonaro['bolsonaro'] / b4_cidades_lula_x_bolsonaro['total_presidente']
    b4_cidades_lula_x_bolsonaro['lula_pct'] = b4_cidades_lula_x_bolsonaro['lula'] / b4_cidades_lula_x_bolsonaro['total_presidente']
    b4_cidades_lula_x_bolsonaro['outros_pct'] = b4_cidades_lula_x_bolsonaro['outro'] / b4_cidades_lula_x_bolsonaro['total_presidente']
    b4_cidades_lula_x_bolsonaro['diferenca_lula_bolsonaro'] = b4_cidades_lula_x_bolsonaro['lula_pct'] - b4_cidades_lula_x_bolsonaro['bolsonaro_pct']
    b4_cidades_lula_x_bolsonaro['razao_lula_bolsonaro'] = b4_cidades_lula_x_bolsonaro['lula_pct'] / b4_cidades_lula_x_bolsonaro['bolsonaro_pct']

    detalhes_municpios = b2_secoes_attendance[['uf','uf_ds','municipio','municipio_ds', 'eleitores_aptos','comparecimento','eleitores_faltosos','pct_comparecimento']]
    detalhes_municpios = detalhes_municpios.groupby(['uf', 'municipio', 'municipio_ds']).agg({'eleitores_aptos': np.sum, 'comparecimento': np.sum, 'eleitores_faltosos': np.sum})
    detalhes_municpios =  detalhes_municpios.reset_index()
    detalhes_municpios['pct_comparecimento'] = detalhes_municpios['comparecimento'] / detalhes_municpios['eleitores_aptos']

    b4_cidades_lula_x_bolsonaro = b4_cidades_lula_x_bolsonaro.set_index(['uf','municipio']).join(detalhes_municpios.set_index(['uf','municipio']), rsuffix='_cidade').reset_index()
else:
    b4_cidades_lula_x_bolsonaro = pd.read_csv(PATH_TO_BASES + "\\"+"b4_cidades_lula_x_bolsonaro.csv")
    b4_cidades_lula_x_bolsonaro = b4_cidades_lula_x_bolsonaro.iloc[:, 1:]

if (not CARREGAR_PROCESSADAS):
        if not os.path.exists(PATH_TO_BASES):
                os.mkdir(PATH_TO_BASES)

        b4_cidades_lula_x_bolsonaro.to_csv(PATH_TO_BASES + "\\"+"b4_cidades_lula_x_bolsonaro.csv")

# Base 5 Votes Fact
del b2_secoes_attendance
del b3_secoes_votting_resume
del b4_cidades_lula_x_bolsonaro

class NoRDVException(Exception):
    def __init__(self, message = "RDV not found"):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)

def get_rdv_data(row):
    _votos = []
    rdv_file = row['rdv']

    if (pd.isnull(rdv_file)):
        raise NoRDVException()

    def new_voto(row, cargo, tipo, digitacao):
        return {'ciclo':row['ciclo'], 'pleito': row['pleito'], 'eleicao': row['eleicao'], 'uf': row['uf'], 'municipio': row['municipio'], 'zona': row['zona'], 'secao': row['secao'], 'cargo': cargo, 'tipo': tipo, 'digitacao': digitacao}

    with open(rdv_file, "rb") as file:
        encoded_rdv = file.read()
        
    resultado_rdv = EntidadeResultadoRDV.load(encoded_rdv)
    rdv = resultado_rdv["rdv"]
    eleicoes = rdv["eleicoes"].chosen
    for eleicao in eleicoes:
        votos_cargos = eleicao["votosCargos"]
        for votos_cargo in votos_cargos:
            votos = votos_cargo["votos"]
            for voto in votos:
                _votos.append(new_voto(row, votos_cargo['idCargo'].chosen.native, voto["tipoVoto"].native, voto["digitacao"].native))
    return _votos


b5_folder_path = "{}\\b5_votos_analitico".format(PATH_TO_BASES)
b5_file_path = "{}\\{}"
b5_filename = "b5_votos_{}_analitico.csv"
b5_error_filename = "b5_errors.csv"
b5_error_line = "RDV not found in uf {}, municipio {}, zona {} and secao {} \n"

if(os.path.exists(b5_folder_path)): shutil.rmtree(b5_folder_path)
os.mkdir(b5_folder_path)


for index, row in b1_secoes_files.iterrows():
    _file_name = b5_filename.format(row['uf'])
    _file_path = b5_file_path.format(b5_folder_path, _file_name)

    try:
        votos_list = get_rdv_data(row)
    except NoRDVException as ex:
        with open(b5_file_path.format(b5_folder_path, b5_error_filename), 'a') as error_file:
            error_file.write(b5_error_line.format(row['uf'], row['municipio'], row['zona'], row['secao']))
        continue

    if(len(votos_list)> 0):
        b5_votos_analitico = pd.DataFrame(votos_list)
        b5_votos_analitico.to_csv(_file_path, mode='a', header=not os.path.exists(_file_path), index=False)