// Importações
import { S3Client, GetObjectCommand, PutObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import mysql from "mysql2/promise";

// configuração s3
const s3 = new S3Client({ region: process.env.AWS_REGION });
const TRUSTED_BUCKET = process.env.TRUSTED_BUCKET;
const CLIENT_BUCKET = process.env.CLIENT_BUCKET;
const CLIENT_KEY = "client_lotes.json";

// configuração banco de dados
const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS || "",
  database: process.env.DB_NAME,
};

// transforma stream em string
const streamToString = (stream) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    stream.on("error", reject);
  });


// handler principal

export const handler = async (event) => {
  try {
    console.log("Iniciando ETL dos LOTES...");

    // conectando ao banco de dados e buscando lotes
    const conn = await mysql.createConnection(dbConfig);
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

    const players = {};
    for (let i = 0; i < rows.length; i++) {
      const p = rows[i];
      players[p.codigo] = {
        lote: String(p.lote),
        empresa: p.empresa,
        modelo: p.modelo,
      };
    }
    console.log("Players mapeados:", Object.keys(players).length);

    // listando todos os CSV do bucket trusted
    const csvKeys = await listarTodosCSV();
    console.log("CSV encontrados:", csvKeys.length);

    const lotes = {};

    // leitura de cada CSV
    for (let i = 0; i < csvKeys.length; i++) {
      const key = csvKeys[i];

      const conteudo = await lerCsv(key);
      if (!conteudo) continue;

      const linha = conteudo.trim();
      const cols = linha.split(",");

      const player = cols[0];
      const cpu = parseFloat(cols[2]);
      const ram = parseFloat(cols[3]);
      const disco = parseFloat(cols[4]);
      const status = cols[10];

      const meta = players[player];
      if (!meta) {
        throw new Error(
          `ERRO: O player ${player} existe no CSV mas não existe no banco ou não tem lote cadastrado.`
        );
      }

      const loteId = meta.lote;

      // criando objeto do lote se ainda não existir
      if (!lotes[loteId]) {
        lotes[loteId] = {
          lote: loteId,
          empresa: meta.empresa,
          modelo: meta.modelo,
          total_players: {},
          players_com_problema: {},
          falhas_por_componente: { cpu: 0, ram: 0, disco: 0 },
        };
      }

      const lote = lotes[loteId];

      // registrando player no lote
      lote.total_players[player] = true;

      // verificando se é problema
      if (status !== "OK") {
        lote.players_com_problema[player] = true;
      }

      // falhas por componente (regras simples)
      if (cpu > 80) lote.falhas_por_componente.cpu++;
      if (ram > 80) lote.falhas_por_componente.ram++;
      if (disco > 90) lote.falhas_por_componente.disco++;
    }

    // criando lista final de lotes
    // aqui calculamos score, status e falhas
    const listaLotes = [];

    for (const loteId in lotes) {
      const l = lotes[loteId];

      const totalPlayers = Object.keys(l.total_players).length;
      const playersProblema = Object.keys(l.players_com_problema).length;

      let percentual = 0;
      if (totalPlayers > 0) percentual = (playersProblema / totalPlayers) * 100;

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

    // preparando lista de scores para mediana
    const scores = [];
    for (let i = 0; i < listaLotes.length; i++) {
      scores.push(listaLotes[i].score);
    }

    for (let i = 0; i < scores.length - 1; i++) {
      for (let j = 0; j < scores.length - i - 1; j++) {
        if (scores[j] > scores[j + 1]) {
          const temp = scores[j];
          scores[j] = scores[j + 1];
          scores[j + 1] = temp;
        }
      }
    }

    // mediana
    let mediana = 0;
    if (scores.length > 0) {
      const meio = Math.floor(scores.length / 2);
      if (scores.length % 2 === 0) {
        mediana = (scores[meio - 1] + scores[meio]) / 2;
      } else {
        mediana = scores[meio];
      }
    }

    // lotes problemáticos
    let lotesProblematicos = 0;
    for (let i = 0; i < listaLotes.length; i++) {
      if (listaLotes[i].status !== "OK") {
        lotesProblematicos++;
      }
    }

    // lotes saudáveis
    let saudaveis = 0;
    for (let i = 0; i < listaLotes.length; i++) {
      if (listaLotes[i].status === "OK") {
        saudaveis++;
      }
    }

    let percentualSaudaveis = 0;
    if (listaLotes.length > 0) {
      percentualSaudaveis = Math.round((saudaveis / listaLotes.length) * 100);
    }

    // bloco da dashboard KPI's
    const dashboard = {
      lotes_problematicos: lotesProblematicos,
      lotes_saudaveis: percentualSaudaveis,
      reclamacoes_criticas: 0, // placeholder para futuras reclamações
      mediana_score_lotes: mediana,
    };

    // salvando o arquivo final no bucket client
    await s3.send(
      new PutObjectCommand({
        Bucket: CLIENT_BUCKET,
        Key: CLIENT_KEY,
        Body: JSON.stringify({ dashboard, lotes: listaLotes }, null, 2),
        ContentType: "application/json",
      })
    );

    console.log("client_lotes.json gerado com sucesso!");
    return { status: "OK" };

  } catch (erro) {
    console.error("ERRO NA ETL:", erro);
    throw erro;
  } finally {
    if (conn && conn.end) {
      try {
        await conn.end();
      } catch (e) {
        console.warn("Aviso: erro ao fechar conexão DB", e);
      }
    }
  }
};


// auxiliares

async function listarTodosCSV() {
  let keys = [];
  let token = undefined;

  do {
    const resp = await s3.send(
      new ListObjectsV2Command({
        Bucket: TRUSTED_BUCKET,
        Prefix: "trusted/",
        ContinuationToken: token,
      })
    );

    for (let i = 0; i < (resp.Contents || []).length; i++) {
      const obj = resp.Contents[i];
      if (obj.Key.endsWith(".csv")) keys.push(obj.Key);
    }

    token = resp.IsTruncated ? resp.NextContinuationToken : undefined;
  } while (token);

  return keys;
}


async function lerCsv(key) {
  const resp = await s3.send(
    new GetObjectCommand({
      Bucket: TRUSTED_BUCKET,
      Key: key,
    })
  );

  return await streamToString(resp.Body);
}
