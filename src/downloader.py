import os
import json
import requests
import logging
import datetime

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

class Downloader:
    def __init__(self, max_workers=1) -> None:
        self.poll = []
        self.inital_size = 0
        self.current_size = 0
        self.max_workers = max_workers
    
    def add(self, item):
        self.poll.append(item)

    def clear(self):
        del self.poll
        self.poll = []

    def run(self, max_workers=None):
        def _worker(url, path):
            return (requests.get(url, allow_redirects=True), path)

        if(max_workers is not None):
            self.max_workers = max_workers
        with ThreadPoolExecutor(max_workers=self.max_workers) as exe:
            #Setup
            self.inital_size = len(self.poll)
            self.current_size = self.inital_size
            self.initial_time = datetime.datetime.now()


            logging.info('Running downloader for {} urls'.format(self.inital_size))
            futures = [exe.submit(_worker, *i) for i in self.poll]
            self.poll = []

            for future in as_completed(futures):
                self.current_size = self.current_size - 1

                #print("{} of {} left".format(self.current_size, self.inital_size,))
                r, path = future.result()
                if not r.status_code == 200:
                    logging.info("URL {} returned staus {}".format(r.url, r.status_code))
                    continue
                
                if not os.path.exists(path):
                    os.makedirs(path)

                filename = "no_name.json"
                if r.url.find('/'):
                    filename = r.url.rsplit('/', 1)[1]
                file_path = path + "/" + filename

                with open(file_path, 'wb') as f:
                    f.write(r.content)
                    f.close()

            self.clear()

URL_CONFIG_FILE = "https://resultados.tse.jus.br/oficial/comum/config/ele-c.json"
URL_CONFIG_ELEICAO = 'https://resultados.tse.jus.br/{}/{}/{}/config/mun-e{}-cm.json'
URL_CONFIG_MUNICIPIOS = 'https://resultados.tse.jus.br/{}/{}/arquivo-urna/{}/config/{}/{}-p{}-cs.json'
URL_DADOS_URNA = 'https://resultados.tse.jus.br/{}/{}/arquivo-urna/{}/dados/{}/{}/{}/{}/{}'
URL_DADOS_URNA_ARQUIVO = 'https://resultados.tse.jus.br/{}/{}/arquivo-urna/{}/dados/{}/{}/{}/{}/{}/{}'


def _get_pleitos(download_folder):
    F1_FILE_PATH = "{}/{}".format(download_folder, "ele-c.json")
    if (not os.path.isfile(F1_FILE_PATH)):
        raise Exception("F1 file does not exists in {}".format(F1_FILE_PATH)) 

    CICLO = ""
    PLEITOS = []

    with open(F1_FILE_PATH, 'r', encoding='utf-8') as f_json:
        json_obj = json.load(f_json)
        CICLO = json_obj['c']
        PLEITOS = json_obj['pl']
    
    return (CICLO, PLEITOS)

def stage_one(downloader, download_folder) :
    downloader.add((URL_CONFIG_FILE, download_folder))
    downloader.run()

def stage_two(downloader, download_folder, allowed_elections=[]):
    CICLO, PLEITOS = _get_pleitos(download_folder)
    for pleito in PLEITOS:
        cd_pleito = pleito['cd']
        for eleicao in pleito['e']:
            cd_eleicao = eleicao['cd']
            #Atuar somente nas eleições listadas
            if(len(allowed_elections) > 0):
                if(cd_eleicao not in allowed_elections):
                    continue

            eleicao_config_path = "{}/{}/{}/config".format(download_folder, CICLO, cd_eleicao)
            a_url_config_eleicao = URL_CONFIG_ELEICAO.format("oficial", CICLO, cd_eleicao, cd_eleicao.zfill(6))
            downloader.add((a_url_config_eleicao, eleicao_config_path))
        downloader.run()

def stage_three(downloader, download_folder):
    #STAGE 3
    #GET ALL VOTING SECTIONS
    CICLO, PLEITOS = _get_pleitos(download_folder)
    for pleito in PLEITOS:
        cd_pleito = pleito['cd']
        for eleicao in pleito['e']:
            cd_eleicao = eleicao['cd']
            F2_CONFIG_PATH = "{}/{}/{}/config".format(download_folder, CICLO, cd_eleicao)

            F2_FILE_PATH = "{}/{}".format(F2_CONFIG_PATH, 'mun-e{}-cm.json'.format(cd_eleicao.zfill(6)))
            if (not os.path.isfile(F2_FILE_PATH)):
                continue
            logging.info(F2_FILE_PATH)
            
            with open(F2_FILE_PATH, 'r', encoding='utf-8') as f_json:
                F3_CONFIG_PATH = "{}/{}/arquivo-urna/{}/config".format(download_folder, CICLO, cd_pleito)

                municipios_json = json.load(f_json)
                for uf in municipios_json['abr']:
                    UF_CODE = uf['cd'].lower()
                    F3_CONFIG_PATH_UF = "{}/{}".format(F3_CONFIG_PATH,UF_CODE)
                    a_url_config_municipios = URL_CONFIG_MUNICIPIOS.format("oficial", CICLO, cd_pleito, UF_CODE, UF_CODE, cd_pleito.zfill(6))
                    downloader.add((a_url_config_municipios, F3_CONFIG_PATH_UF))
    downloader.run()

def stage_four(downloader, download_folder):
    #STAGE 4
    #GET CONFIG FILE FOR EACH VOTING MACHINE
    CICLO, PLEITOS = _get_pleitos(download_folder)
    for pleito in PLEITOS:
        cd_pleito = pleito['cd']
        for eleicao in pleito['e']:
            cd_eleicao = eleicao['cd']
            F2_CONFIG_PATH = "{}/{}/{}/config".format(download_folder, CICLO, cd_eleicao)

            F2_FILE_PATH = "{}/{}".format(F2_CONFIG_PATH, 'mun-e{}-cm.json'.format(cd_eleicao.zfill(6)))
            if (not os.path.isfile(F2_FILE_PATH)):
                continue
            
            with open(F2_FILE_PATH, 'r', encoding='utf-8') as f_json:
                F3_CONFIG_PATH = "{}/{}/arquivo-urna/{}/config".format(download_folder, CICLO, cd_pleito)

                municipios_json = json.load(f_json)
                
                for uf in municipios_json['abr']:
                    UF_CODE = uf['cd'].lower()
                    
                    F3_CONFIG_PATH_UF = "{}/{}".format(F3_CONFIG_PATH,UF_CODE)
                    F3_FILE = '{}/{}'.format(F3_CONFIG_PATH_UF, '{}-p{}-cs.json'.format(UF_CODE, cd_pleito.zfill(6)))
                    
                    with open(F3_FILE, 'r', encoding='utf-8') as esf_json:
                        estado_json = json.load(esf_json)
                        for cidade in estado_json['abr'][0]['mu']:
                            cd_cidade = cidade['cd']
                            for zona in cidade['zon']:
                                cd_zona = zona['cd']
                                for secao in zona['sec']:
                                    cd_secao = secao['ns']
                                    
                                    DADOS_URNA_PATH = "{}/{}/arquivo-urna/{}/dados/{}/{}/{}/{}".format(download_folder, CICLO, cd_pleito, UF_CODE, cd_cidade, cd_zona.zfill(4), cd_secao.zfill(4))
                                    DADOS_URNA_FILENAME = 'p{}-{}-m{}-z{}-s{}-aux.json'.format(cd_pleito.zfill(6), UF_CODE, cd_cidade.zfill(5),cd_zona.zfill(4), cd_secao.zfill(4))
                                    a_url_dados_urna = URL_DADOS_URNA.format("oficial", CICLO, cd_pleito, UF_CODE, cd_cidade, cd_zona.zfill(4), cd_secao.zfill(4), DADOS_URNA_FILENAME)
                                    
                                    if (os.path.isfile('{}/{}'.format(a_url_dados_urna, DADOS_URNA_FILENAME))):
                                        continue

                                    downloader.add((a_url_dados_urna, DADOS_URNA_PATH))
                    downloader.run()
        

def stage_five(downloader, download_folder):
    #GET CONFIG FILE FOR EACH VOTING MACHINE
    CICLO, PLEITOS = _get_pleitos(download_folder)
    for pleito in PLEITOS:
        cd_pleito = pleito['cd']
        for eleicao in pleito['e']:
            cd_eleicao = eleicao['cd']
            F2_CONFIG_PATH = "{}/{}/{}/config".format(downloader, CICLO, cd_eleicao)

            F2_FILE_PATH = "{}/{}".format(F2_CONFIG_PATH, 'mun-e{}-cm.json'.format(cd_eleicao.zfill(6)))
            if (not os.path.isfile(F2_FILE_PATH)):
                continue
            
            with open(F2_FILE_PATH, 'r', encoding='utf-8') as f_json:
                F3_CONFIG_PATH = "{}/{}/arquivo-urna/{}/config".format(downloader, CICLO, cd_pleito)

                municipios_json = json.load(f_json)
                
                for uf in municipios_json['abr']:
                    UF_CODE = uf['cd'].lower()
                    #TODO RETIRAR RESTRIÇÂO POR UF
                    #if(UF_CODE != 'ac'):
                        #continue
                    
                    F3_CONFIG_PATH_UF = "{}/{}".format(F3_CONFIG_PATH,UF_CODE)
                    F3_FILE = '{}/{}'.format(F3_CONFIG_PATH_UF, '{}-p{}-cs.json'.format(UF_CODE, cd_pleito.zfill(6)))
                    
                    with open(F3_FILE, 'r', encoding='utf-8') as esf_json:
                        estado_json = json.load(esf_json)
                        for cidade in estado_json['abr'][0]['mu']:
                            cd_cidade = cidade['cd']
                            for zona in cidade['zon']:
                                cd_zona = zona['cd']
                                for secao in zona['sec']:
                                    cd_secao = secao['ns']
                                    
                                    DADOS_URNA_PATH = "{}/{}/arquivo-urna/{}/dados/{}/{}/{}/{}".format(downloader, CICLO, cd_pleito, UF_CODE, cd_cidade, cd_zona.zfill(4), cd_secao.zfill(4))
                                    URNA_DADOS_FILENAME = 'p{}-{}-m{}-z{}-s{}-aux.json'.format(cd_pleito.zfill(6), UF_CODE, cd_cidade.zfill(5),cd_zona.zfill(4), cd_secao.zfill(4))

                                    with open('{}/{}'.format(DADOS_URNA_PATH, URNA_DADOS_FILENAME), 'r', encoding='utf-8') as esf_json:
                                        urna_files_json = json.load(esf_json)
                                        urna_hash = urna_files_json['hashes'][0]['hash']
                                        DADOS_URNA_HASH_PATH = '{}/{}'.format(DADOS_URNA_PATH, urna_hash)
                                        if(urna_files_json['hashes'][0]['st'] == "Sem arquivo"):
                                            continue
                                        for file in urna_files_json['hashes'][0]['nmarq']:
                                            if (os.path.isfile('{}/{}'.format(DADOS_URNA_HASH_PATH, file))):
                                                continue

                                            a_url_dados_urna_aquivo = URL_DADOS_URNA_ARQUIVO.format("oficial", CICLO, cd_pleito, UF_CODE, cd_cidade, cd_zona.zfill(4), cd_secao.zfill(4), urna_hash, file)

                                            downloader.add((a_url_dados_urna_aquivo, DADOS_URNA_HASH_PATH))
                            downloader.run()

        





#Run Script
def main(downloader_workers=1, allowed_elections=['544'],download_folder="../downloads"):
    my_downloader = Downloader(downloader_workers)

    stage_one(my_downloader, download_folder)
    stage_two(my_downloader, download_folder)
    stage_three(my_downloader, download_folder)
    stage_four(my_downloader, download_folder)
    stage_five(my_downloader, download_folder)

if __name__ == '__main__':
    main(downloader_workers=4, allowed_elections=['544'],download_folder="../downloads")