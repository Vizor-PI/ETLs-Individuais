import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";

// Inicializa o s3
const s3 = new S3Client({ region: "us-east-1" });

const DEST_BUCKET = "vizor-client";

export const handler = async (event) => {
  try {
    const srcBucket = event.Records[0].s3.bucket.name;
    // Decodificando caracteres especiais
    const srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    console.log(`Arquivo recebido: ${srcBucket}/${srcKey}`);

    // Constante para identificar as partes do arquivo do trusted
    const pathParts = srcKey.split('/');
    
    // Validando se está na pasta esperada do trusted
    if (pathParts.length < 4) {
      console.log("Arquivo fora da pasta esperada.");
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

    console.log(`JSON de Dashboard salvo em: ${DEST_BUCKET}/${destKey}`);
    return { statusCode: 200, body: "Sucesso" };

  } catch (error) {
    console.error("Erro na ETL JS:", error);
    return { statusCode: 500, body: error.message };
  }
};

// Função auxiliar para gerar texto bonito pro dashboard
function determinarMensagemErro(m) {
  if (m.cpu > 80) return "Uso de processador extremamente alto.";
  if (m.ram > 80) return "Memória RAM no limite.";
  if (m.disk > 90) return "Disco quase cheio. Risco de travamento.";
  if (m.temp > 75) return "Temperatura está muito alta!.";
  return "Hardware com valores elevados.";
}