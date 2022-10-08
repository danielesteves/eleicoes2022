import logging


from tse import rdv_dump, rdv_resumo
from tse.rdv_resumo import *

RDV_PATH = "data/o00406-5877700290295.rdv"
ASN1_PATH = "tse/spec/rdv.asn1"

#rdv_dump.processa_rdv(ASN1_PATH, RDV_PATH)
#rdv_resumo.imprime_rdv(RDV_PATH)

def carrega_rdv(rdv_path: str):
    logging.debug("Lendo arquivo %s", rdv_path)
    with open(rdv_path, "rb") as file:
        encoded_rdv = file.read()
    resultado_rdv = EntidadeResultadoRDV.load(encoded_rdv)
    rdv = resultado_rdv["rdv"]
    eleicoes = rdv["eleicoes"].chosen
    print("=" * 40)
    for eleicao in eleicoes:
        votos_cargos = eleicao["votosCargos"]
        for votos_cargo in votos_cargos:
            qtd = 0
            print("-" * 40)
            print(f"{votos_cargo['idCargo'].chosen.native}")
            votos = votos_cargo["votos"]
            for voto in votos:
                qtd += 1
                digitacao = voto["digitacao"]
                tipo = voto["tipoVoto"]
                if digitacao == asn1.VOID:
                    print(f"{qtd:3} - {tipo.native}")
                else:
                    print(f"{qtd:3} - {tipo.native:8} - [{digitacao}]")
    print("=" * 40)
    return resultado_rdv

resultado_rdv = carrega_rdv(RDV_PATH)
