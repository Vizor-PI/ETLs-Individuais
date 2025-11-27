// Importações
import { S3Client, GetObjectCommand, PutObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import mysql from "mysql2/promise";

// configuração s3
const s3 = new S3Client({ region: process.env.AWS_REGION });
const TRUSTED_BUCKET = process.env.TRUSTED_BUCKET;
const CLIENT_BUCKET = process.env.CLIENT_BUCKET;

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
  let conn;

  try {
    console.log("Iniciando ETL dos LOTES...");

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

    console.log("Players mapeados:", Object.keys(players).length);

    // listando CSVs
    const csvKeys = await listarTodosCSV();
    console.log("CSV encontrados:", csvKeys.length);

    const lotes = {};

    for (let idx = 0; idx < csvKeys.length; idx++) {
      const key = csvKeys[idx];
      const conteudo = await lerCsv(key);
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
          throw new Error(`Player ${player} não existe no banco.`);
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

      console.log(`Dashboard gerado: ${dashboardKey}`);

      // json por lote
      for (const lote of lotesEmpresa) {

        const loteKey = `${empresaNome}/${lote.lote}/lote.json`;

        await s3.send(new PutObjectCommand({
          Bucket: CLIENT_BUCKET,
          Key: loteKey,
          Body: JSON.stringify(lote, null, 2),
          ContentType: "application/json",
        }));

        console.log(`Arquivo por lote gerado: ${loteKey}`);
      }
    }

    return { status: "OK" };

  } catch (erro) {
    console.error("ERRO NA ETL:", erro);
    throw erro;

  } finally {
    if (conn && conn.end) {
      try {
        await conn.end();
      } catch (e) {
        console.warn("Aviso ao fechar conexão:", e);
      }
    }
  }
};


// auxiliares

async function listarTodosCSV() {
  const arquivos = [];

  async function listar(prefixo) {
    const comando = new ListObjectsV2Command({
      Bucket: TRUSTED_BUCKET,
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

    // desce mais níveis se houver "pasta" simulada
    for (const obj of resposta.Contents) {
      if (obj.Key.endsWith("/")) {
        await listar(obj.Key);
      }
    }
  }

  // começa da raiz do bucket
  await listar("");

  return arquivos;
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
