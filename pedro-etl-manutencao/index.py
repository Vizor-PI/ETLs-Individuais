import json
import math
import os
from statistics import median
from datetime import datetime, timedelta
from collections import defaultdict
# pip install boto3
import boto3

cliente_s3 = boto3.client("s3")

# bucket de destino -> onde vão os JSONs que o Node vai ler
BALDE_DESTINO = os.environ.get("DEST_BUCKET", "vizor-client")


def lambda_handler(evento, contexto):
    try:
        #pega as infos básicas do evento S3 (quem chamou o Lambda)
        registro = evento["Records"][0]
        balde_origem = registro["s3"]["bucket"]["name"]
        #nome do arquivo que chegou no trusted
        chave_origem = registro["s3"]["object"]["key"].replace("+", " ")

        print(f"[ETL] Arquivo recebido: s3://{balde_origem}/{chave_origem}")

        #esperamos algo do tipo: empresa/maquina/data/arquivo.csv
        partes_caminho = chave_origem.split("/")
        if len(partes_caminho) < 4:
            print("Arquivo fora da pasta esperada (empresa/maquina/data/...).")
            return {"statusCode": 200, "body": "Ignorado"}

        empresa = partes_caminho[0]     
        maquina_id = partes_caminho[1]  

        # lê o CSV do bucket trusted
        resposta = cliente_s3.get_object(Bucket=balde_origem, Key=chave_origem)
        conteudo_csv = resposta["Body"].read().decode("utf-8")

        linhas = [l for l in conteudo_csv.strip().split("\n") if l.strip()]
        if len(linhas) < 2:
            print("CSV sem dados (só cabeçalho?).")
            return {"statusCode": 200, "body": "CSV vazio"}

        cabecalho = linhas[0]     
        linhas_dados = linhas[1:]

        # listas gerais para montar medianas, regressão e histórico
        lista_datas = []
        lista_cpu = []
        lista_ram = []
        lista_disco = []
        lista_temp = []
        lista_prob_falha_hist = []

        formato_data = "%Y-%m-%d %H:%M:%S"

        # varre cada linha do CSV e extrai os campos que importam
        for linha in linhas_dados:
            colunas = [c.strip() for c in linha.split(",")]
            if len(colunas) < 11:
                continue

            texto_timestamp = colunas[1]

            try:
                texto_timestamp_limpo = texto_timestamp.split(".")[0]
                data_linha = datetime.strptime(texto_timestamp_limpo, formato_data)
            except Exception:
                # se não der pra converter a data, a gente ignora essa linha
                continue

            cpu_linha = _limpar_float(colunas[2])
            ram_linha = _limpar_float(colunas[3])
            disco_linha = _limpar_float(colunas[4])
            temp_linha = _limpar_float(colunas[6])

            lista_datas.append(data_linha)
            lista_cpu.append(cpu_linha)
            lista_ram.append(ram_linha)
            lista_disco.append(disco_linha)
            lista_temp.append(temp_linha)

            # probabilidade histórica de falha 
            prob_hist = _calcular_prob_falha_simples(temp=temp_linha, disco=disco_linha)
            lista_prob_falha_hist.append(prob_hist)

        if not lista_datas:
            print("Nenhuma linha com timestamp válido.")
            return {"statusCode": 200, "body": "Sem timestamps válidos"}

        # data mais recente do CSV (vamos usar isso para chavear por dia)
        data_maxima = max(lista_datas)

        # pega a última linha capturada (estado mais recente da máquina)
        ultima_linha = linhas_dados[-1]
        colunas_ultima = [c.strip() for c in ultima_linha.split(",")]

        cpu_atual = _limpar_float(colunas_ultima[2])
        ram_atual = _limpar_float(colunas_ultima[3])
        disco_atual = _limpar_float(colunas_ultima[4])
        uptime_atual = colunas_ultima[5]
        temp_atual = _limpar_float(colunas_ultima[6])
        timestamp_atual = colunas_ultima[1]
        situacao_atual = colunas_ultima[8] if len(colunas_ultima) > 8 else "Desconhecido"
        latitude = _limpar_float(colunas_ultima[9]) if len(colunas_ultima) > 9 else 0.0
        longitude = _limpar_float(colunas_ultima[10]) if len(colunas_ultima) > 10 else 0.0

        # status resumido pro mapa e cards
        situacao_lower = (situacao_atual or "").lower()
        if situacao_lower == "critico":
            status_resumido = "critico"
        elif situacao_lower == "alerta":
            status_resumido = "alerta"
        else:
            status_resumido = "ok"

        # pacotinho com as métricas atuais 
        metricas_atuais = {
            "cpu": cpu_atual,
            "ram": ram_atual,
            "disk": disco_atual,
            "uptime": uptime_atual,
            "temp": temp_atual,
            "timestamp": timestamp_atual,
            "situacao": situacao_atual,
            "latitude": latitude,
            "longitude": longitude,
        }

        # medianas do dia e da última semana (pras KPIs)
        data_ref = data_maxima.date() 

        medianas_dia = {
            "cpu": _mediana_ultimo_dia(lista_cpu, lista_datas, data_ref),
            "ram": _mediana_ultimo_dia(lista_ram, lista_datas, data_ref),
            "disco": _mediana_ultimo_dia(lista_disco, lista_datas, data_ref),
            "temp": _mediana_ultimo_dia(lista_temp, lista_datas, data_ref),
        }

        inicio_semana = data_maxima - timedelta(days=7)
        medianas_semanal = {
            "cpu": _mediana_periodo(lista_cpu, lista_datas, inicio_semana),
            "ram": _mediana_periodo(lista_ram, lista_datas, inicio_semana),
            "disco": _mediana_periodo(lista_disco, lista_datas, inicio_semana),
            "temp": _mediana_periodo(lista_temp, lista_datas, inicio_semana),
        }

        bloco_medianas = {
            "dia": medianas_dia,
            "semanal": medianas_semanal
        }

        # regressão da probabilidade de falha (tendência ao longo do tempo)
        regressao_risco = _calcular_regressao_probabilidade(lista_prob_falha_hist)

        # bloco de UI e modelo heurístico de risco
        estado_ui = _montar_ui_state(metricas_atuais, maquina_id)
        modelo_heuristico = _calcular_modelo_heuristico(metricas_atuais)

        # histórico real dos últimos 7 dias 
        historico_7d = _montar_historico_7d(
            lista_datas,
            lista_cpu,
            lista_ram,
            lista_disco,
            lista_temp,
            lista_prob_falha_hist,
            inicio_semana,
        )

        # monta o JSON que o Node vai consumir
        dashboard_json = {
            "machine_id": maquina_id,         
            "company": empresa,
            "status": status_resumido,       
            "last_update": metricas_atuais["timestamp"],
            "raw_metrics": {
                "cpu": f"{metricas_atuais['cpu']:.1f}%",
                "ram": f"{metricas_atuais['ram']:.1f}%",
                "disco": f"{metricas_atuais['disk']:.1f}%",
                "temp": f"{metricas_atuais['temp']:.1f}°C",
                "uptime": metricas_atuais["uptime"],
                "latitude": metricas_atuais["latitude"],
                "longitude": metricas_atuais["longitude"],
            },
            "ui": estado_ui,
            "risk_model": modelo_heuristico,
            "medianas": bloco_medianas,
            "regressao_risco": regressao_risco,
            "historico_7d": historico_7d,
        }

        # define a chave de destino no bucket client
        data_str = data_maxima.strftime("%Y-%m-%d")
        chave_destino = f"pedro-client/{empresa}/{data_str}/{maquina_id}.json"

        cliente_s3.put_object(
            Bucket=BALDE_DESTINO,
            Key=chave_destino,
            Body=json.dumps(dashboard_json, ensure_ascii=False, indent=2),
            ContentType="application/json",
        )

        print(f"JSON de Dashboard salvo em: s3://{BALDE_DESTINO}/{chave_destino}")
        return {"statusCode": 200, "body": "Sucesso"}

    except Exception as erro:
        print("Erro na ETL Python:", erro)
        return {"statusCode": 500, "body": str(erro)}


# funções auxiliares
def _limpar_float(valor):
    if valor is None or valor == "":
        return 0.0
    try:
        return float(str(valor).replace(",", ".").strip())
    except ValueError:
        return 0.0


def _mediana_ultimo_dia(lista_valores, lista_datas, data_ref):
    valores = [
        v for v, d in zip(lista_valores, lista_datas)
        if d.date() == data_ref
    ]
    if not valores:
        return 0.0
    return float(median(valores))


def _mediana_periodo(lista_valores, lista_datas, inicio_periodo):
    valores = [
        v for v, d in zip(lista_valores, lista_datas)
        if d >= inicio_periodo
    ]
    if not valores:
        return 0.0
    return float(median(valores))


def _calcular_prob_falha_simples(temp, disco):
    pontuacao_hw = (temp * 0.7) + (disco * 0.3)
    prob_falha = min(99, math.floor(pontuacao_hw))
    return float(prob_falha)


def _calcular_regressao_probabilidade(lista_prob):
    n = len(lista_prob)
    if n == 0:
        return {
            "inclinacao": 0.0,
            "intercepto": 0.0,
            "tendencia": "estavel",
            "prob_atual_regressao": 0.0,
            "prob_proxima_regressao": 0.0,
        }

    if n == 1:
        prob = _limitar_prob(lista_prob[0])
        return {
            "inclinacao": 0.0,
            "intercepto": prob,
            "tendencia": "estavel",
            "prob_atual_regressao": prob,
            "prob_proxima_regressao": prob,
        }

    lista_x = list(range(n))
    soma_x = sum(lista_x)
    soma_y = sum(lista_prob)
    soma_x2 = sum(x * x for x in lista_x)
    soma_xy = sum(x * y for x, y in zip(lista_x, lista_prob))

    denominador = (n * soma_x2) - (soma_x ** 2)
    if denominador == 0:
        prob = _limitar_prob(lista_prob[-1])
        return {
            "inclinacao": 0.0,
            "intercepto": prob,
            "tendencia": "estavel",
            "prob_atual_regressao": prob,
            "prob_proxima_regressao": prob,
        }

    inclinacao = (n * soma_xy - soma_x * soma_y) / denominador
    intercepto = (soma_y - inclinacao * soma_x) / n

    x_atual = n - 1
    x_proximo = n

    prob_atual_reg = _limitar_prob(inclinacao * x_atual + intercepto)
    prob_proxima_reg = _limitar_prob(inclinacao * x_proximo + intercepto)

    if inclinacao > 0.5:
        tendencia = "subindo"
    elif inclinacao < -0.5:
        tendencia = "descendo"
    else:
        tendencia = "estavel"

    return {
        "inclinacao": float(inclinacao),
        "intercepto": float(intercepto),
        "tendencia": tendencia,
        "prob_atual_regressao": prob_atual_reg,
        "prob_proxima_regressao": prob_proxima_reg,
    }


def _limitar_prob(valor):
    return max(0.0, min(99.0, float(valor)))


def _montar_ui_state(metricas, maquina_id):
    situacao = metricas["situacao"]
    cpu = metricas["cpu"]
    ram = metricas["ram"]
    temp = metricas["temp"]

    estado_ui = {
        "severity": "INFO",
        "color": "green",
        "icon": "check-circle",
        "title": "Operação Normal",
        "message": "Monitoramento ativo. Parâmetros estáveis.",
        "action": "Nenhuma ação necessária",
    }

    if situacao == "Critico" or cpu > 90 or ram > 90 or temp > 75:
        estado_ui = {
            "severity": "CRITICO",
            "color": "red",
            "icon": "alert-triangle",
            "title": f"Falha Crítica em {maquina_id}",
            "message": _determinar_mensagem_erro(metricas),
            "action": "Intervenção Imediata / Reboot Forçado",
        }
    elif situacao == "Alerta" or cpu > 70 or ram > 70:
        estado_ui = {
            "severity": "ALERTA",
            "color": "yellow",
            "icon": "alert-circle",
            "title": "Atenção Requerida",
            "message": _determinar_mensagem_erro(metricas),
            "action": "Verificar processos ou limpar cache",
        }

    return estado_ui


def _determinar_mensagem_erro(metricas):
    if metricas["cpu"] > 80:
        return "Uso de processador extremamente alto."
    if metricas["ram"] > 80:
        return "Memória RAM no limite."
    if metricas["disk"] > 90:
        return "Disco quase cheio. Risco de travamento."
    if metricas["temp"] > 75:
        return "Temperatura está muito alta!."
    return "Hardware com valores elevados."


def _calcular_modelo_heuristico(metricas):
    cpu = metricas["cpu"]
    ram = metricas["ram"]
    disco = metricas["disk"]
    temp = metricas["temp"]
    status = (metricas["situacao"] or "").lower()

    pontuacao_hw = (temp * 0.7) + (disco * 0.3)
    prob_falha = min(99, math.floor(pontuacao_hw))

    pontuacao_sw = (cpu * 0.6) + (ram * 0.4)
    nivel_estresse = math.floor(pontuacao_sw)

    dias_manutencao = "45+ dias"
    if status == "critico" or prob_falha > 80:
        dias_manutencao = "IMEDIATA"
    elif prob_falha > 60:
        dias_manutencao = "7 dias"
    elif prob_falha > 40:
        dias_manutencao = "15 dias"

    causa = "Desgaste Natural"
    recomendacao = "Monitoramento Padrão"
    nivel_risco = "low"

    if status == "critico" or prob_falha > 80:
        nivel_risco = "high"
        if temp > 80:
            causa = "Estresse Térmico (Perigo)"
            recomendacao = "Verificar Arrefecimento"
        else:
            causa = "Falha de Hardware Iminente"
            recomendacao = "Agendar Troca de Player"
    elif status == "alerta" or prob_falha > 60:
        nivel_risco = "medium"
        causa = "Operação em Limite Térmico"
        recomendacao = "Monitorar temperatura ambiente"
    elif nivel_estresse > 85:
        causa = "Sobrecarga de Software"
        recomendacao = "Reiniciar ou Otimizar Conteúdo"

    return {
        "prob": float(prob_falha),
        "stress": float(nivel_estresse),
        "days": dias_manutencao,
        "cause": causa,
        "rec": recomendacao,
        "riskLevel": nivel_risco,
    }


def _montar_historico_7d(lista_datas, lista_cpu, lista_ram, lista_disco,
                        lista_temp, lista_prob, inicio_periodo):

    por_dia_cpu = defaultdict(list)
    por_dia_ram = defaultdict(list)
    por_dia_disco = defaultdict(list)
    por_dia_temp = defaultdict(list)
    por_dia_prob = defaultdict(list)

    for data, cpu, ram, disco, temp, prob in zip(
        lista_datas, lista_cpu, lista_ram, lista_disco, lista_temp, lista_prob
    ):
        if data < inicio_periodo:
            continue
        dia = data.date()
        por_dia_cpu[dia].append(cpu)
        por_dia_ram[dia].append(ram)
        por_dia_disco[dia].append(disco)
        por_dia_temp[dia].append(temp)
        por_dia_prob[dia].append(prob)

    if not por_dia_cpu:
        return {
            "labels": [],
            "cpu": [],
            "ram": [],
            "disco": [],
            "temp": [],
            "prob_falha": [],
        }

    dias_ordenados = sorted(por_dia_cpu.keys())
    dias_ordenados = dias_ordenados[-7:]

    labels = [d.strftime("%Y-%m-%d") for d in dias_ordenados]
    hist_cpu = [float(median(por_dia_cpu[d])) for d in dias_ordenados]
    hist_ram = [float(median(por_dia_ram[d])) for d in dias_ordenados]
    hist_disco = [float(median(por_dia_disco[d])) for d in dias_ordenados]
    hist_temp = [float(median(por_dia_temp[d])) for d in dias_ordenados]
    hist_prob = [float(median(por_dia_prob[d])) for d in dias_ordenados]

    return {
        "labels": labels,
        "cpu": hist_cpu,
        "ram": hist_ram,
        "disco": hist_disco,
        "temp": hist_temp,
        "prob_falha": hist_prob,
    }
