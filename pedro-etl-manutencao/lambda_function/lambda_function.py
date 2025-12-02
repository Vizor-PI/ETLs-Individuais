import json
import math
import os
from statistics import median
from datetime import datetime, timedelta
from collections import defaultdict
import boto3

cliente_s3 = boto3.client("s3")
BALDE_DESTINO = os.environ.get("DEST_BUCKET", "vizor-client")

def lambda_handler(evento, contexto):
    print("--- INICIO ETL PYTHON (PEDRO) ---")
    print(f"Evento recebido: {json.dumps(evento)}")

    try:
        # 1. Extração do Evento
        if "Records" not in evento:
            print("ERRO: Evento sem Records. Ignorando.")
            return {"statusCode": 400, "body": "Sem Records"}
            
        registro = evento["Records"][0]
        balde_origem = registro["s3"]["bucket"]["name"]
        chave_original = registro["s3"]["object"]["key"].replace("+", " ")

        print(f"Tentando baixar: s3://{balde_origem}/{chave_original}")

        # 2. Leitura Inteligente do S3 (Tenta com e sem prefixo 'trusted/')
        conteudo_csv = None
        chave_final = chave_original

        try:
            # Tentativa 1: Chave exata do evento
            resp = cliente_s3.get_object(Bucket=balde_origem, Key=chave_original)
            conteudo_csv = resp["Body"].read().decode("utf-8")
        except cliente_s3.exceptions.NoSuchKey:
            print(f"AVISO: Arquivo não encontrado em '{chave_original}'.")
            
            # Tentativa 2: Remove 'trusted/' se existir, ou adiciona se não existir
            if chave_original.startswith("trusted/"):
                chave_final = chave_original.replace("trusted/", "", 1)
            else:
                chave_final = f"trusted/{chave_original}"
            
            print(f"Tentando alternativa: '{chave_final}'")
            try:
                resp = cliente_s3.get_object(Bucket=balde_origem, Key=chave_final)
                conteudo_csv = resp["Body"].read().decode("utf-8")
                print("SUCESSO: Arquivo encontrado na tentativa alternativa.")
            except Exception as e:
                print(f"ERRO FATAL: Arquivo não existe nem como '{chave_original}' nem '{chave_final}'.")
                raise e

        # 3. Processamento do Caminho (Para definir empresa e máquina)
        # Removemos 'trusted/' apenas para parsing lógico
        caminho_limpo = chave_final
        if caminho_limpo.startswith("trusted/"):
            caminho_limpo = caminho_limpo.replace("trusted/", "", 1)
            
        partes = caminho_limpo.split("/")
        # Esperado: Empresa/Maquina/Data/arquivo.csv
        if len(partes) < 2:
            print("ERRO: Estrutura de pastas inválida.")
            return {"statusCode": 400, "body": "Path invalido"}

        empresa = partes[0]
        maquina_id = partes[1]
        print(f"Processando para Empresa: {empresa}, Máquina: {maquina_id}")

        # 4. Parsing do CSV
        linhas = [l for l in conteudo_csv.strip().split("\n") if l.strip()]
        if len(linhas) < 2:
            print("ERRO: CSV vazio ou apenas cabeçalho.")
            return {"statusCode": 200, "body": "CSV vazio"}

        linhas_dados = linhas[1:]
        
        # Listas para estatísticas
        dados = defaultdict(list)
        prob_falhas = []
        timestamps_validos = []

        # Formatos de data suportados (Brasileiro e ISO)
        def parse_data(t):
            t = t.split(".")[0].strip()
            fmts = ["%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S"]
            for fmt in fmts:
                try: return datetime.strptime(t, fmt)
                except: continue
            return None

        # Loop de Dados
        for linha in linhas_dados:
            cols = [c.strip() for c in linha.split(",")]
            if len(cols) < 7: continue

            dt = parse_data(cols[1]) # Coluna Timestamp
            if not dt: continue

            # Índices baseados no seu CSV: CPU(2), RAM(3), Disco(4), Temp(6)
            cpu = _safe_float(cols[2])
            ram = _safe_float(cols[3])
            disco = _safe_float(cols[4])
            temp = _safe_float(cols[6])

            dados["cpu"].append(cpu)
            dados["ram"].append(ram)
            dados["disco"].append(disco)
            dados["temp"].append(temp)
            timestamps_validos.append(dt)
            
            # Cálculo de Probabilidade Histórica (Simples)
            prob = min(99, math.floor((temp * 0.7) + (disco * 0.3)))
            prob_falhas.append(prob)

        if not timestamps_validos:
            print("ERRO: Nenhum timestamp válido encontrado no CSV.")
            return {"statusCode": 200, "body": "Sem dados validos"}

        # 5. Dados Atuais (Última Linha)
        ult_cols = linhas_dados[-1].split(",")
        # Mapeamento do seu CSV para o JSON
        metricas_atuais = {
            "cpu": _safe_float(ult_cols[2]),
            "ram": _safe_float(ult_cols[3]),
            "disk": _safe_float(ult_cols[4]),
            "uptime": ult_cols[5],
            "temp": _safe_float(ult_cols[6]),
            "timestamp": ult_cols[1],
            "situacao": ult_cols[8] if len(ult_cols) > 8 else "Normal",
            # Indices 9 e 10 para Lat/Long conforme seu CSV
            "latitude": _safe_float(ult_cols[9]) if len(ult_cols) > 9 else 0.0,
            "longitude": _safe_float(ult_cols[10]) if len(ult_cols) > 10 else 0.0
        }

        # 6. Cálculos de Inteligência (Medianas e Regressão)
        data_max = max(timestamps_validos)
        dt_ref = data_max.date()
        dt_sem = data_max - timedelta(days=7)

        # Helper para medianas
        def calc_med(lista, datas, filtro_func):
            vals = [v for v, d in zip(lista, datas) if filtro_func(d)]
            return float(median(vals)) if vals else 0.0

        medianas = {
            "dia": {
                "cpu": calc_med(dados["cpu"], timestamps_validos, lambda d: d.date() == dt_ref),
                "ram": calc_med(dados["ram"], timestamps_validos, lambda d: d.date() == dt_ref),
                "temp": calc_med(dados["temp"], timestamps_validos, lambda d: d.date() == dt_ref)
            },
            "semanal": {
                "cpu": calc_med(dados["cpu"], timestamps_validos, lambda d: d >= dt_sem),
                "ram": calc_med(dados["ram"], timestamps_validos, lambda d: d >= dt_sem),
                "temp": calc_med(dados["temp"], timestamps_validos, lambda d: d >= dt_sem)
            }
        }

        # Regressão Linear Simples para Risco
        regressao = _calcular_regressao(prob_falhas)
        
        # Modelo Heurístico
        modelo_risco = _calcular_modelo_heuristico(metricas_atuais)

        # Montagem do JSON Final
        dashboard_json = {
            "machine_id": maquina_id,
            "company": empresa,
            "status": "critico" if metricas_atuais["situacao"].lower() == "critico" else "ok",
            "last_update": metricas_atuais["timestamp"],
            "raw_metrics": {
                "cpu": f"{metricas_atuais['cpu']:.1f}%",
                "ram": f"{metricas_atuais['ram']:.1f}%",
                "disco": f"{metricas_atuais['disk']:.1f}%",
                "temp": f"{metricas_atuais['temp']:.1f}°C",
                "latitude": metricas_atuais["latitude"],
                "longitude": metricas_atuais["longitude"]
            },
            "risk_model": modelo_risco,
            "medianas": medianas,
            "regressao_risco": regressao
        }

        # 7. Upload
        # Define caminho: pedro-client/Empresa/Data/Maquina.json
        data_str = data_max.strftime("%d-%m-%Y")
        chave_destino = f"pedro-client/{empresa}/{data_str}/{maquina_id}.json"

        print(f"Salvando JSON em: s3://{BALDE_DESTINO}/{chave_destino}")
        
        cliente_s3.put_object(
            Bucket=BALDE_DESTINO,
            Key=chave_destino,
            Body=json.dumps(dashboard_json, ensure_ascii=False, indent=2),
            ContentType="application/json"
        )

        return {"statusCode": 200, "body": "Sucesso Python"}

    except Exception as e:
        print(f"ERRO FATAL PYTHON: {str(e)}")
        # Importante: Retornar erro 200 com mensagem de erro evita retries infinitos no Lambda assíncrono, 
        # mas para debug é melhor ver o log.
        return {"statusCode": 500, "body": str(e)}

# --- Funções Auxiliares ---
def _safe_float(v):
    try: return float(str(v).replace(",", "."))
    except: return 0.0

def _calcular_modelo_heuristico(m):
    # Lógica simplificada para exemplo
    score = (m["temp"] * 0.6) + (m["disk"] * 0.4)
    risco = "low"
    if score > 80: risco = "high"
    elif score > 50: risco = "medium"
    
    return {
        "prob": score,
        "riskLevel": risco,
        "cause": "Sobrecarga" if m["cpu"] > 80 else "Desgaste",
        "rec": "Verificar refrigeração" if m["temp"] > 70 else "Monitorar"
    }

def _calcular_regressao(valores):
    # Regressão linear simplificada (y = ax + b)
    if not valores: return {"tendencia": "estavel"}
    n = len(valores)
    x = list(range(n))
    y = valores
    soma_x = sum(x)
    soma_y = sum(y)
    soma_xx = sum(i*i for i in x)
    soma_xy = sum(i*j for i,j in zip(x,y))
    
    den = (n * soma_xx - soma_x * soma_x)
    if den == 0: return {"tendencia": "estavel", "inclinacao": 0}
    
    a = (n * soma_xy - soma_x * soma_y) / den
    tendencia = "subindo" if a > 0.1 else ("descendo" if a < -0.1 else "estavel")
    
    return {"inclinacao": a, "tendencia": tendencia}