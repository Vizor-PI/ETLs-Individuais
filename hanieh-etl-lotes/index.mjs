import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client({ region: "us-east-1" });

const DEST_BUCKET = "vizor-client";

export const handler = async (event) => {
  try {
    const srcBucket = event.Records[0].s3.bucket.name;
    const srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    console.log(`Arquivo recebido: ${srcBucket}/${srcKey}`);

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
    const destKey = srcKey.replace(/^trusted\//, "alertas/").replace(/\.csv$/, ".json");

    const putCommand = new PutObjectCommand({
      Bucket: DEST_BUCKET,
      Key: destKey,
      Body: JSON.stringify(alerts, null, 2),
      ContentType: "application/json"
    });

    await s3.send(putCommand);

    console.log(`JSON de alertas salvo em: ${DEST_BUCKET}/${destKey}`);
    return { statusCode: 200, body: "Sucesso" };

  } catch (error) {
    console.error("Erro na ETL JS:", error);
    return { statusCode: 500, body: error.message };
  }
};