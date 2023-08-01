import wget
import zipfile
import os
import shutil
import sqlite3
import dask.dataframe as dd

FILES_NAMES = ["Cnaes.zip", "Empresas0.zip", "Empresas1.zip", "Empresas2.zip", "Empresas3.zip",
                            "Empresas4.zip", "Empresas5.zip", "Empresas6.zip", "Empresas7.zip", "Empresas8.zip",
                            "Empresas9.zip", "Estabelecimentos0.zip", "Estabelecimentos1.zip",
                            "Estabelecimentos2.zip", "Estabelecimentos3.zip", "Estabelecimentos4.zip",
                            "Estabelecimentos5.zip", "Estabelecimentos6.zip", "Estabelecimentos7.zip",
                            "Estabelecimentos8.zip", "Estabelecimentos9.zip", "Motivos.zip", "Municipios.zip",
                            "Naturezas.zip", "Paises.zip", "Qualificacoes.zip", "Simples.zip", "Socios0.zip",
                            "Socios1.zip", "Socios2.zip", "Socios3.zip", "Socios4.zip", "Socios5.zip", "Socios6.zip",
                            "Socios7.zip", "Socios8.zip", "Socios9.zip"]
BLOCKSIZEPROCESSING = "500MB"

class DBConstructor():
    """
    A classe DBConstructor realiza o download, extração e tratamento em SQLITE de todos os arquivos relacionados
    dos dados abertos de CNPJs do Brasil.
    https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj
    """
    def __init__(self, update=False):
        if update:
            self.update()

    def downloadfiles(self):
        """
        Esse método utiliza a biblioteca wget para fazer o download de todos os arquivos com os nomes
        escritos na variável global FILES_NAMES. Nomes de arquivos coletados em:
        https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj
        """
        for file in FILES_NAMES:
            if file in os.listdir():
                continue
            url = f"https://dadosabertos.rfb.gov.br/CNPJ/{file}"
            wget.download(url, file)
            
    def extractfiles(self):
        """
        Os dados baixados pelo método downloadfiles são extraidos utilizando a biblioteca zipfile.
        Todos os arquivos extraidos são nomeados conforme as necessidades da classe e movidos para a raiz do código.
        Os arquivos compactados são apagados.
        """
        atualcwd = os.getcwd()

        # extraindo arquivos, movendo para a pasta raiz do código, apagando o arquivo zipado.
        for file in FILES_NAMES:
            with zipfile.ZipFile(file, 'r') as zip_ref:
                zip_ref.extractall(f"{atualcwd}\{file.replace('.zip', '')}")
            origem = os.path.join(atualcwd, file.replace(".zip", ""))
            downloadedname = os.listdir(origem)[0]
            shutil.move(os.path.join(origem, downloadedname),
                        atualcwd)
        for dir in os.listdir():
            path = os.path.join(atualcwd, dir)
            if os.path.isdir(path):
                os.rmdir(path)
        for zipfil in os.listdir():
            if ".zip" in zipfil:
                os.remove(os.path.join(atualcwd, zipfil))
        
        # Renomeando os arquivos csv com base em suas referências.
        socio = 0
        empresa = 0
        estabelecimento = 0
        for file in os.listdir():
            if "SIMPLES" in file:
                os.rename(os.path.join(atualcwd, file), "simples.csv")
            if "CNAE" in file:
                os.rename(os.path.join(atualcwd, file), "cnae.csv")
            if "MOTI" in file:
                os.rename(os.path.join(atualcwd, file), "motivos.csv")
            if "MUNIC" in file:
                os.rename(os.path.join(atualcwd, file), "municipios.csv")
            if "NATJU" in file:
                os.rename(os.path.join(atualcwd, file), "natureza_juridica.csv")
            if "PAIS" in file:
                os.rename(os.path.join(atualcwd, file), "pais.csv")
            if "QUALS" in file:
                os.rename(os.path.join(atualcwd, file), "qualificacoes.csv")
            if "EMPRE" in file:
                os.rename(os.path.join(atualcwd, file), f"empresas{empresa}.csv")
                empresa += 1
            if "ESTABELE" in file:
                os.rename(os.path.join(atualcwd, file), f"estabelecimento{estabelecimento}.csv")
                estabelecimento += 1
            if "SOCIO" in file:
                os.rename(os.path.join(atualcwd, file), f"socio{socio}.csv")
                socio += 1

    def creatdb(self): 
        """
        Essa função cria um banco de dados sqlite a partir dos CSVs coletados. Os arquivos manipulados neste método tem tamanho expressivo,
        podem ocorrer erros de memória em computadores de baixa performance.
        """
        # conectando ao banco de dados
        dbname = "cnpjinfos.db"
        if dbname in os.listdir():
            os.remove(dbname)
        conn = sqlite3.connect(dbname)

        # Definindo tipos, criando tabelas e adicionado dados na tabela utilizando pandas.
        for file in os.listdir():
            print(file)
            if 'empresas' in file:
                dtypes = {
                    "CNPJ_BASICO": str,
                    "RAZAO_SOCIAL": str,
                    "NATUREZA_JURIDICA": str,
                    "QUALIFICACAO_DO_RESPONSAVEL":str,
                    "CAPITAL_SOCIAL":str,
                    "PORTE": str,
                    "ENTE_FEDERATIVO": str
                }
                colnames = list(dtypes.keys())
                df = dd.read_csv(file, sep=';', encoding='latin-1', header=None, names=colnames,
                                 dtype=dtypes, low_memory=False, blocksize=BLOCKSIZEPROCESSING)
                df["CAPITAL_SOCIAL"] = df["CAPITAL_SOCIAL"].apply(lambda x: x.replace(',', '.'),
                                                                  meta=('CAPITAL_SOCIAL', 'float'))
                df.compute().to_sql("EMPRESAS", conn, if_exists='append', index=False)
            if "estabelecimento" in file:
                dtypes = {
                    "CNPJ_BASICO":str,
                    "CNPJ_ORDEM":str,
                    "CNPJ_DV":str,
                    "MATRIZ_FILIAL": str,
                    "NOME_FANTASIA": str,
                    "SITUACAO_CADASTRAL":str,
                    "DATA_SITUACAO": str,
                    "MOTIVO_SITUACAO": str,
                    "NOME_CIDADE_EXTERIOR": str,
                    "PAIS": str,
                    "INICIO_ATIVIDADE": str,
                    "CNAE_PRINCIPAL":str,
                    "CNAE_SECUNDARIO":str,
                    "LOGRADOURO_TIPO":str,
                    "LOGRADOURO":str,
                    "NUMERO":str,
                    "COMPLEMENTO":str,
                    "BAIRRO":str,
                    "CEP":str,
                    "UF":str,
                    "MUNICIPIO":str,
                    "DDD1":str,
                    "TELEFONE1":str,
                    "DD2":str,
                    "TELEFONE2":str,
                    "DDDFAX":str,
                    "FAX":str,
                    "CORREIO_ELETRONICO":str,
                    "SITUACAO_ESPECIAL":str,
                    "DATA_SITUACAO_ESPECIAL":str
                }
                colnames = list(dtypes.keys())
                sep = ';'
                df = dd.read_csv(file, sep=sep, encoding='latin-1', header=None, names=colnames,
                                 dtype=dtypes, blocksize=BLOCKSIZEPROCESSING, on_bad_lines='warn')
                df["DATA_SITUACAO"] = dd.to_datetime(df['DATA_SITUACAO'], format="%Y%m%d",
                                                     errors='coerce')
                df["INICIO_ATIVIDADE"] = dd.to_datetime(df["INICIO_ATIVIDADE"], format="%Y%m%d",
                                                        errors='coerce')
                df["DATA_SITUACAO_ESPECIAL"] = dd.to_datetime(df["DATA_SITUACAO_ESPECIAL"], format="%Y%m%d",
                                                            errors='coerce')
                df.compute().to_sql("ESTABELECIMENTOS", conn, if_exists='append', index=False)     
            if "simples" in file:
                dtypes = {"CNPJ_BASICO":str,
                          "OPCAO_SIMPLES":str,
                          "DATA_OPCAO_SIMPLES":str,
                          "DATA_EXCLUSAO_SIMPLES":str,
                          "OPCAO_MEI":str,
                          "DATA_OPCAO_MEI":str,
                          "DATA_EXCLUSAO_MEI":str}
                colnames = list(dtypes.keys())
                df = dd.read_csv(file, sep=';', encoding='latin-1', header=None, names=colnames,
                                 dtype=dtypes, low_memory=False, blocksize=BLOCKSIZEPROCESSING)
                df['DATA_OPCAO_SIMPLES'] = dd.to_datetime(df['DATA_OPCAO_SIMPLES'], format="%Y%m%d", errors='coerce')
                df['DATA_EXCLUSAO_SIMPLES'] = dd.to_datetime(df['DATA_OPCAO_SIMPLES'], format="%Y%m%d", errors='coerce')
                df['DATA_OPCAO_MEI'] = dd.to_datetime(df['DATA_OPCAO_SIMPLES'], format="%Y%m%d", errors='coerce')
                df['DATA_EXCLUSAO_MEI'] = dd.to_datetime(df['DATA_OPCAO_SIMPLES'], format="%Y%m%d", errors='coerce')
                df.compute().to_sql("SIMPLES", conn, if_exists='append', index=False)
            if "socio" in file:
                dtypes = {"CNPJ_BASICO": str,
                          "IDENTIFICADOR_SOCIO": str,
                          "NOME_SOCIO":str,
                          "CPFCNPJ_SOCIO":str,
                          "QUALIFICACAO_SOCIO":str,
                          "DATA_ENTRADA_SOCIEDADE":str,
                          "PAIS":str,
                          "CPF_REPRESENTANTE_LEGAL":str,
                          "NOME_REPRESENTANTE_LEGAL":str,
                          "QUALIFICACAO_REPRESENTANTE_LEGAL":str,
                          "FAIXA_ETARIA":str}
                
                colnames = list(dtypes.keys())
                df = dd.read_csv(file, sep=';', encoding='latin-1', header=None, names=colnames,
                                 dtype=dtypes, low_memory=False, blocksize=BLOCKSIZEPROCESSING)
                df["DATA_ENTRADA_SOCIEDADE"] = dd.to_datetime(df["DATA_ENTRADA_SOCIEDADE"], format="%Y%m%d", errors="coerce")
                df.compute().to_sql("SOCIOS", conn, if_exists='append', index=False)         
            if "pais" in file:
                colnames = ["CODIGO", "DESCRICAO"]
                df = dd.read_csv(file, sep=';', encoding='latin-1', header=None, names=colnames,
                                 dtype=str, low_memory=False, blocksize=BLOCKSIZEPROCESSING)
                df.compute().to_sql("PAISES", conn, if_exists='append', index=False)         
            if "municipio" in file:
                colnames = ["CODIGO", "DESCRICAO"]
                df = dd.read_csv(file, sep=';', encoding='latin-1', header=None, names=colnames,
                                 dtype=str, low_memory=False, blocksize=BLOCKSIZEPROCESSING)
                df.compute().to_sql('MUNICIPIOS', conn, if_exists="append", index=False)
            if "qualificacoes" in file:
                colnames = ["CODIGO", "DESCRICAO"]
                df = dd.read_csv(file, sep=';', encoding='latin-1', header=None, names=colnames,
                                 dtype=str, low_memory=False, blocksize=BLOCKSIZEPROCESSING)
                df.compute().to_sql("QUALIFICACOES", conn, if_exists='append', index=False)      
            if 'natureza' in file:
                colnames = ["CODIGO", "DESCRICAO"]
                df = dd.read_csv(file, sep=';', encoding='latin-1', header=None, names=colnames,
                                 dtype=str, low_memory=False, blocksize=BLOCKSIZEPROCESSING)
                df.compute().to_sql("NATUREZAS", conn, if_exists='append', index=False)       
            if "cnae" in file:
                colnames = ["CODIGO", "DESCRICAO"]
                df = dd.read_csv(file, sep=';', encoding='latin-1', header=None, names=colnames,
                                 dtype=str, low_memory=False, blocksize=BLOCKSIZEPROCESSING)
                df.compute().to_sql("CNAES", conn, if_exists="append", index=False)   
            if "motivo" in file:
                colnames = ["CODIGO", "DESCRICAO"]
                df = dd.read_csv(file, sep=';', encoding='latin-1', header=None, names=colnames,
                                 dtype=str, low_memory=False, blocksize=BLOCKSIZEPROCESSING)
                df.compute().to_sql('MOTIVOS', conn, if_exists='append', index=False)

        # Encerrando o conector
        conn.close()

    def update(self):
        """
        Reinicia a base de dados.
        """
        for file in os.listdir():
            if ('csv' in file) or ('.db' in file):
                os.remove(file)
        self.downloadfiles()
        self.extractfiles()
        self.creatdb()
        self.removecsv()

    def removecsv(self):
        """
        Exclui todos os arquivos CSV da pasta raiz do código.
        """
        for file in os.listdir():
            if 'csv' in file:
                os.remove(file)
 
if __name__ == "__main__":
    db_constructor = DBConstructor(True)