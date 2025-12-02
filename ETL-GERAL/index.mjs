import { S3Client, GetObjectCommand, PutObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import mysql from "mysql2/promise";

// Inicializa o s3 
const s3 = new S3Client({ region: "us-east-1" });

// Configurações Globais
const DEST_BUCKET = "vizor-client";


// Configuração banco de dados (Miguel)
const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS || "",
  database: process.env.DB_NAME,
};


// HANDLER PRINCIPAL 
export const handler = async (event) => {
    console.log("Iniciando processamento unificado...");

    try {
        // Executa todas as funções em paralelo para performance
        // Se uma falhar, as outras continuam tentando se tratar o erro individualmente dentro delas.
        await Promise.all([
            aquinoToClient(event),
            haniehToClient(event),
            miguelToClient(event)
        ]);

        return { statusCode: 200, body: "Processamento completo das 3 rotinas." };

    } catch (error) {
        console.error("Erro fatal no Handler Principal:", error);
        return { statusCode: 500, body: error.message };
    }
};


// ____________________AQUINO (Dashboard Incidentes)____________________

async function aquinoToClient(event) {
  try {
    const srcBucket = event.Records[0].s3.bucket.name;
    // Decodificando caracteres especiais
    const srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    console.log(`[Aquino] Arquivo recebido: ${srcBucket}/${srcKey}`);

    // Constante para identificar as partes do arquivo do trusted
    const pathParts = srcKey.split('/');
    
    // Validando se está na pasta esperada do trusted
    if (pathParts.length < 4) {
      console.log("[Aquino] Arquivo fora da pasta esperada.");
      return;
    }

    const empresa = pathParts[0];   
    const maquinaId = pathParts[1]; 
    // const data = pathParts[2];  //estou pegando o timestamp do CSV

    // Baixando o CSV do trusted
    const getCommand = new GetObjectCommand({
      Bucket: srcBucket,
      Key: srcKey,
    });
    const response = await s3.send(getCommand);
    const csvContent = await response.Body.transformToString();

    const lines = csvContent.trim().split('\n'); // Pegando o último Status
    const lastLine = lines[lines.length - 1]; // Pegando a última linha
    const cols = lastLine.split(','); // Separando as colunas

     const cleanFloat = (val) => {
        if (!val) return 0;
        return parseFloat(val.trim());
    };

    // Organização das metricas:
    // 0:User, 1:Time, 2:CPU, 3:Mem, 4:Disco, 5:Uptime, 6:Temp, 7:Indoor, 8:Situacao, 9:Lat, 10:Long
    const metrics = {
      cpu: parseFloat(cols[2]),
      ram: parseFloat(cols[3]),
      disk: parseFloat(cols[4]),
      uptime: cols[5],
      temp: parseFloat(cols[6]), 
      timestamp: cols[1],
      situacao: cols[8] ? cols[8].trim() : "Desconhecido",
      latitude: cleanFloat(cols[9]),
      longitude: cleanFloat(cols[10])
    };

    // Gerando User Interface baseado nas métricas
    let uiState = {
      severity: "INFO",
      color: "green", // green, yellow, red
      icon: "check-circle",
      title: "Operação Normal",
      message: "Monitoramento ativo. Parâmetros estáveis.",
      action: "Nenhuma ação necessária"
    };

    // Lógica baseada em 'Situacao' que veio do Java 
    if (metrics.situacao === "Critico" || metrics.cpu > 90 || metrics.ram > 90 || metrics.temp > 75) {
      uiState = {
        severity: "CRITICO",
        color: "red",
        icon: "alert-triangle",
        title: `Falha Crítica em ${maquinaId}`,
        message: determinarMensagemErro(metrics),
        action: "Intervenção Imediata / Reboot Forçado"
      };
    } else if (metrics.situacao === "Alerta" || metrics.cpu > 70 || metrics.ram > 70) {
      uiState = {
        severity: "ALERTA",
        color: "yellow",
        icon: "alert-circle",
        title: "Atenção Requerida",
        message: determinarMensagemErro(metrics),
        action: "Verificar processos ou limpar cache"
      };
    }

    // JSON Final
    const dashboardJson = {
      machine_id: maquinaId,
      company: empresa,
      last_update: metrics.timestamp,
      raw_metrics: {
        cpu: `${metrics.cpu.toFixed(1)}%`,
        ram: `${metrics.ram.toFixed(1)}%`,
        disco: `${metrics.disk.toFixed(1)}%`,
        temp: `${metrics.temp.toFixed(1)}°C`,
        latitude: metrics.latitude,
        longitude: metrics.longitude
      },
      ui: uiState
    };

    // Salvando no Vizor-Client ("aquino-client/Empresa/MaquinaID.json")
    const destKey = `aquino-client/${empresa}/${maquinaId}.json`;

    const putCommand = new PutObjectCommand({
      Bucket: DEST_BUCKET,
      Key: destKey,
      Body: JSON.stringify(dashboardJson, null, 2),
      ContentType: "application/json"
    });

    await s3.send(putCommand);

    console.log(`[Aquino] JSON de Dashboard salvo em: ${DEST_BUCKET}/${destKey}`);

  } catch (error) {
    console.error("[Aquino] Erro na ETL JS:", error);
    // Não lança erro para não parar as outras funções
  }
}

// Função auxiliar - Aquino
function determinarMensagemErro(m) {
  if (m.cpu > 80) return "Uso de processador extremamente alto.";
  if (m.ram > 80) return "Memória RAM no limite.";
  if (m.disk > 90) return "Disco quase cheio. Risco de travamento.";
  if (m.temp > 75) return "Temperatura está muito alta!.";
  return "Hardware com valores elevados.";
}


// ____________________HANIEH (Alertas Individuais para Lotes)____________________

async function haniehToClient(event) {
  try {
    const srcBucket = event.Records[0].s3.bucket.name;
    const srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    console.log(`[Hanieh] Arquivo recebido: ${srcBucket}/${srcKey}`);

    // Baixando o CSV do trusted
    const getCommand = new GetObjectCommand({
      Bucket: srcBucket,
      Key: srcKey,
    });
    const response = await s3.send(getCommand);
    const csvContent = await response.Body.transformToString();

    const lines = csvContent.trim().split('\n');
    const header = lines[0].split(',');
    const alerts = [];

    // Descobrindo os índices dos campos (caso a ordem mude)
    const idx = (name) => header.findIndex(h => h.trim().toLowerCase() === name.toLowerCase());

    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(',');

      const timestamp = cols[idx("Timestamp")];
      const modelo = cols[idx("Modelo")];
      const lote = cols[idx("Lote")];
      const indoor = cols[idx("Indoor")];

      const cpu = parseFloat(cols[idx("CPU")]);
      const mem = parseFloat(cols[idx("Memoria")]);
      const disco = parseFloat(cols[idx("Disco")]);
      const temp = parseFloat(cols[idx("Temperatura")]);

      // Função igual à do Java
      const classificarSeveridade = (tipo, v) => {
        switch (tipo) {
          case "CPU": return v > 85 ? "CRITICO" : v > 70 ? "ATENCAO" : "NORMAL";
          case "MEMORIA": return v > 90 ? "CRITICO" : v > 75 ? "ATENCAO" : "NORMAL";
          case "DISCO": return v > 95 ? "CRITICO" : v > 80 ? "ATENCAO" : "NORMAL";
          case "TEMPERATURA": return v > 80 ? "CRITICO" : v > 70 ? "ATENCAO" : "NORMAL";
          default: return "NORMAL";
        }
      };

      // Para cada métrica, gerar alerta se necessário
      [
        { tipo: "CPU", valor: cpu },
        { tipo: "MEMORIA", valor: mem },
        { tipo: "DISCO", valor: disco },
        { tipo: "TEMPERATURA", valor: temp }
      ].forEach(({ tipo, valor }) => {
        const severidade = classificarSeveridade(tipo, valor);
        if (severidade !== "NORMAL") {
          alerts.push({
            timestamp,
            modelo,
            lote,
            tipo,
            valor,
            severidade,
            indoor
          });
        }
      });
    }

    // Salvando o array de alertas no bucket de destino
    const destKeySufixo = srcKey.replace(/^trusted\//, "alertas/").replace(/\.csv$/, ".json");
    const PASTA_DESTINO = "hanieh-client/";
    const finalKey = PASTA_DESTINO + destKeySufixo;

    const putCommand = new PutObjectCommand({
      Bucket: DEST_BUCKET,
      Key: finalKey,
      Body: JSON.stringify(alerts, null, 2),
      ContentType: "application/json"
    });

    await s3.send(putCommand);

    console.log(`[Hanieh] JSON de alertas salvo em: ${DEST_BUCKET}/${finalKey}`);

  } catch (error) {
    console.error("[Hanieh] Erro na ETL JS:", error);
    // Não lança erro para não parar as outras funções
  }
}


// ____________________MIGUEL____________________

async function miguelToClient(event) {
  let conn;

 
  const TRUSTED_BUCKET = event.Records[0].s3.bucket.name;
  const CLIENT_BUCKET = DEST_BUCKET; // 

  try {
    console.log("[Miguel] Iniciando ETL dos LOTES...");

    // conectando ao banco
    conn = await mysql.createConnection(dbConfig);
    const [rows] = await conn.execute(`
      SELECT mc.codigo AS codigo,
             l.id AS lote,
             e.nome AS empresa,
             m.nome AS modelo
      FROM miniComputador mc
      LEFT JOIN lote l ON mc.fkLote = l.id
      LEFT JOIN empresa e ON l.fkEmpresa = e.id
      LEFT JOIN modelo m ON l.fkModelo = m.id
    `);

    // mapeamento
    const players = {};
    for (let i = 0; i < rows.length; i++) {
      const p = rows[i];
      players[p.codigo] = {
        lote: String(p.lote),
        empresa: p.empresa,
        modelo: p.modelo,
      };
    }

    console.log("[Miguel] Players mapeados:", Object.keys(players).length);

    // listando CSVs
    const csvKeys = await listarTodosCSV(TRUSTED_BUCKET);
    console.log("[Miguel] CSV encontrados:", csvKeys.length);

    const lotes = {};

    for (let idx = 0; idx < csvKeys.length; idx++) {
      const key = csvKeys[idx];
      const conteudo = await lerCsv(TRUSTED_BUCKET, key);
      if (!conteudo) continue;

      const linhas = conteudo.trim().split("\n");

      for (let i = 1; i < linhas.length; i++) {
        const cols = linhas[i].split(",");

        const player = cols[0];
        const cpu = parseFloat(cols[2]);
        const ram = parseFloat(cols[3]);
        const disco = parseFloat(cols[4]);
        const temperatura = parseFloat(cols[6]);
        const status = cols[8];

        const meta = players[player];
        if (!meta) {
          // Player não existe no banco, pula ou loga
          // throw new Error(`Player ${player} não existe no banco.`); // Comentei para não quebrar o fluxo
          continue; 
        }

        const loteId = meta.lote;

        // cria lote se necessário
        if (!lotes[loteId]) {
          lotes[loteId] = {
            lote: loteId,
            empresa: meta.empresa,
            modelo: meta.modelo,
            total_players: {},
            players_com_problema: {},
            falhas_por_componente: { cpu: 0, ram: 0, disco: 0 }
          };
        }

        const lote = lotes[loteId];

        lote.total_players[player] = true;

        if (status !== "OK") {
          lote.players_com_problema[player] = true;
        }

        if (cpu > 80) lote.falhas_por_componente.cpu++;
        if (ram > 80) lote.falhas_por_componente.ram++;
        if (disco > 90) lote.falhas_por_componente.disco++;
      }
    }

    const listaLotes = [];

    for (const loteId in lotes) {
      const l = lotes[loteId];

      const totalPlayers = Object.keys(l.total_players).length;
      const playersProblema = Object.keys(l.players_com_problema).length;

      let percentual = totalPlayers > 0 ? (playersProblema / totalPlayers) * 100 : 0;
      const score = Math.round(100 - percentual);

      let status = "OK";
      if (score < 50) status = "Crítico";
      else if (score < 70) status = "Alerta";

      const falhasTotais =
        l.falhas_por_componente.cpu +
        l.falhas_por_componente.ram +
        l.falhas_por_componente.disco;

      listaLotes.push({
        lote: l.lote,
        empresa: l.empresa,
        modelo: l.modelo,
        total_players: totalPlayers,
        players_com_problema: playersProblema,
        percentual_problemas: Number(percentual.toFixed(1)),
        score,
        status,
        falhas_totais: falhasTotais,
        falhas_por_componente: l.falhas_por_componente
      });
    }

    // KPI's
    const scores = listaLotes.map(l => l.score).sort((a, b) => a - b);

    let mediana = 0;
    if (scores.length > 0) {
      const m = Math.floor(scores.length / 2);
      mediana = scores.length % 2 === 0 ? (scores[m - 1] + scores[m]) / 2 : scores[m];
    }

    const lotesProblematicos = listaLotes.filter(l => l.status !== "OK").length;
    const saudaveis = listaLotes.filter(l => l.status === "OK").length;
    const percentualSaudaveis = listaLotes.length > 0
      ? Math.round((saudaveis / listaLotes.length) * 100)
      : 0;

    const dashboard = {
      lotes_problematicos: lotesProblematicos,
      lotes_saudaveis: percentualSaudaveis,
      reclamacoes_criticas: 0,
      mediana_score_lotes: mediana,
    };

    // jsons

    // agrupar lotes por empresa
    const empresas = {};
    for (const lote of listaLotes) {
      if (!empresas[lote.empresa]) {
        empresas[lote.empresa] = [];
      }
      empresas[lote.empresa].push(lote);
    }

    // gerar dashboard e arquivos por lote
    for (const empresaNome in empresas) {

      const lotesEmpresa = empresas[empresaNome];

      // json kpi's da empresa
      const scores = lotesEmpresa.map(l => l.score).sort((a, b) => a - b);

      let mediana = 0;
      if (scores.length > 0) {
        const m = Math.floor(scores.length / 2);
        mediana = scores.length % 2 === 0 ? (scores[m - 1] + scores[m]) / 2 : scores[m];
      }

      const lotesProblematicos = lotesEmpresa.filter(l => l.status !== "OK").length;
      const saudaveis = lotesEmpresa.filter(l => l.status === "OK").length;
      const percentualSaudaveis = lotesEmpresa.length > 0
        ? Math.round((saudaveis / lotesEmpresa.length) * 100)
        : 0;

      const dashboardEmpresa = {
        lotes_problematicos: lotesProblematicos,
        lotes_saudaveis: percentualSaudaveis,
        reclamacoes_criticas: 0, // reclamacoes futuramente
        mediana_score_lotes: mediana
      };

      const dashboardKey = `${empresaNome}/dashboard.json`;

      await s3.send(new PutObjectCommand({
        Bucket: CLIENT_BUCKET,
        Key: dashboardKey,
        Body: JSON.stringify(dashboardEmpresa, null, 2),
        ContentType: "application/json",
      }));

      console.log(`[Miguel] Dashboard gerado: ${dashboardKey}`);

      // json por lote
      for (const lote of lotesEmpresa) {

        const loteKey = `${empresaNome}/${lote.lote}/lote.json`;

        await s3.send(new PutObjectCommand({
          Bucket: CLIENT_BUCKET,
          Key: loteKey,
          Body: JSON.stringify(lote, null, 2),
          ContentType: "application/json",
        }));

        console.log(`[Miguel] Arquivo por lote gerado: ${loteKey}`);
      }
    }

  } catch (erro) {
    console.error("[Miguel] ERRO NA ETL:", erro);
    // Não lança erro para não parar as outras funções, apenas loga

  } finally {
    if (conn && conn.end) {
      try {
        await conn.end();
      } catch (e) {
        console.warn("[Miguel] Aviso ao fechar conexão:", e);
      }
    }
  }
}


// FUNÇÕES AUXILIARES - Miguel


// transforma stream em string
const streamToString = (stream) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    stream.on("error", reject);
  });

async function listarTodosCSV(bucketName) {
  const arquivos = [];

  async function listar(prefixo) {
    const comando = new ListObjectsV2Command({
      Bucket: bucketName,
      Prefix: prefixo
    });

    const resposta = await s3.send(comando);

    if (!resposta.Contents) return;

    // adiciona arquivos CSV encontrados
    for (const obj of resposta.Contents) {
      if (obj.Key.endsWith(".csv")) {
        arquivos.push(obj.Key);
      }
    }


    // Mantendo logica original:
    if (resposta.CommonPrefixes) {
        for (const obj of resposta.CommonPrefixes) {
             await listar(obj.Prefix);
        }
    }
  }

  // começa da raiz do bucket
  await listar("");

  return arquivos;
}

async function lerCsv(bucketName, key) {
  const resp = await s3.send(
    new GetObjectCommand({
      Bucket: bucketName,
      Key: key,
    })
  );

  return await streamToString(resp.Body);
}