require('dotenv').config();
const express = require('express');
const mqtt = require('mqtt');
const admin = require('firebase-admin');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());

// ── Firebase Admin (FCM v1) ────────────────────────────────────────
// FIREBASE_SERVICE_ACCOUNT debe ser el contenido COMPLETO de tu
// serviceAccount.json, pegado como una sola línea en la variable de
// entorno de Railway (no subas el archivo al repo).
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

// ── Tokens FCM persistidos en disco ────────────────────────────────
// En Railway, /data no siempre persiste entre deploys salvo que uses
// un Volume. Si aún no tienes uno, esto al menos sobrevive reinicios
// del mismo contenedor (crashes, sleep). Si quieres persistencia real
// entre deploys, lo ideal es una base de datos pequeña (ej. Railway
// Postgres o SQLite con volumen montado).
const TOKENS_FILE = path.join(__dirname, 'tokens.json');

function loadTokens() {
  try {
    const raw = fs.readFileSync(TOKENS_FILE, 'utf8');
    return new Set(JSON.parse(raw));
  } catch {
    return new Set();
  }
}

function saveTokens() {
  fs.writeFileSync(TOKENS_FILE, JSON.stringify([...pushTokens]), 'utf8');
}

let pushTokens = loadTokens();

// ── Conexión MQTT ───────────────────────────────────────────────────
const mqttClient = mqtt.connect(`mqtts://${process.env.MQTT_HOST}:8883`, {
  username: process.env.MQTT_USER,
  password: process.env.MQTT_PASS,
  rejectUnauthorized: false,
});

mqttClient.on('connect', () => {
  console.log('✓ Conectado al broker MQTT');
  mqttClient.subscribe('baston/estado');
  mqttClient.subscribe('baston/caida');
  mqttClient.subscribe('baston/tokens');
  console.log('✓ Suscrito a los topics');
});

mqttClient.on('message', async (topic, payload) => {
  const msg = payload.toString();
  console.log(`MQTT [${topic}]: ${msg}`);

  if (topic === 'baston/tokens') {
    // Ahora esperamos un FCM token nativo (string largo, no
    // "ExponentPushToken[...]"). Validación básica de longitud/formato.
    if (msg && msg.length > 50) {
      pushTokens.add(msg);
      saveTokens();
      console.log(`Token guardado. Total: ${pushTokens.size}`);
    } else {
      console.warn('Token recibido con formato inesperado, ignorado');
    }
  }

  if (topic === 'baston/estado' && msg === 'OFFLINE') {
    await sendPushNotification({
      title: '⚠️ Dispositivo desconectado',
      body: 'SafeStep perdió la conexión WiFi',
      type: 'DEVICE_OFFLINE',
    });
  }

  if (topic === 'baston/caida') {
    await sendPushNotification({
      title: '🚨 ¡Caída detectada!',
      body: 'SafeStep detectó una posible caída',
      type: 'FALL_ALERT',
    });
  }
});

mqttClient.on('error', (err) => {
  console.error('Error MQTT:', err.message);
});

// ── Mandar notificación push (FCM v1, alta prioridad) ──────────────
async function sendPushNotification({ title, body, type, extra = {} }) {
  if (pushTokens.size === 0) {
    console.log('No hay tokens registrados');
    return;
  }

  const tokensArray = [...pushTokens];
  const invalidTokens = [];

  // FCM v1 no soporta envío multicast masivo en un solo "send()";
  // usamos sendEachForMulticast, que sí procesa un array de tokens
  // en una sola llamada y devuelve el resultado por token.
  const message = {
    tokens: tokensArray,
    notification: { title, body },
    data: {
      type,
      timestamp: String(Date.now()),
      ...Object.fromEntries(
        Object.entries(extra).map(([k, v]) => [k, String(v)])
      ),
    },
    android: {
      priority: 'high',
      notification: {
        channelId: 'fall-alerts',
        sound: 'default',
        defaultVibrateTimings: false,
        vibrateTimingsMillis: [0, 500, 250, 500],
      },
    },
    apns: {
      headers: { 'apns-priority': '10' },
      payload: {
        aps: { sound: 'default', contentAvailable: true },
      },
    },
  };

  try {
    const response = await admin.messaging().sendEachForMulticast(message);
    console.log(
      `Notificaciones: ${response.successCount} ok, ${response.failureCount} fallidas`
    );

    response.responses.forEach((res, i) => {
      if (!res.success) {
        const code = res.error?.code;
        console.error(`Token falló (${tokensArray[i].slice(0, 15)}...): ${code}`);
        // Tokens muertos/desregistrados: hay que limpiarlos
        if (
          code === 'messaging/registration-token-not-registered' ||
          code === 'messaging/invalid-registration-token'
        ) {
          invalidTokens.push(tokensArray[i]);
        }
      }
    });

    if (invalidTokens.length > 0) {
      invalidTokens.forEach((t) => pushTokens.delete(t));
      saveTokens();
      console.log(`Se eliminaron ${invalidTokens.length} tokens inválidos`);
    }
  } catch (error) {
    console.error('Error enviando notificaciones FCM:', error);
  }
}

// ── Webhook de EMQX (respaldo) ──────────────────────────────────────
app.post('/webhook', async (req, res) => {
  const { topic, payload } = req.body;
  console.log(`Webhook recibido [${topic}]: ${payload}`);

  if (topic === 'baston/estado' && payload === 'OFFLINE') {
    await sendPushNotification({
      title: '⚠️ Dispositivo desconectado',
      body: 'SafeStep perdió la conexión WiFi',
      type: 'DEVICE_OFFLINE',
    });
  }

  if (topic === 'baston/caida') {
    await sendPushNotification({
      title: '🚨 ¡Caída detectada!',
      body: 'SafeStep detectó una posible caída',
      type: 'FALL_ALERT',
    });
  }

  res.json({ ok: true });
});

// ── Registrar token vía HTTP (alternativa al topic MQTT) ───────────
app.post('/register-token', (req, res) => {
  const { token } = req.body;
  if (!token || token.length < 50) {
    return res.status(400).json({ error: 'Token inválido' });
  }
  pushTokens.add(token);
  saveTokens();
  res.json({ ok: true, total: pushTokens.size });
});

// ── Ruta para ver tokens guardados ──────────────────────────────────
app.get('/tokens', (req, res) => {
  res.json({ tokens: [...pushTokens], total: pushTokens.size });
});

// ── Ruta de prueba ───────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({ status: 'SafeStep Server corriendo', tokens: pushTokens.size });
});

// ── Iniciar servidor ──────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`✓ Servidor corriendo en puerto ${PORT}`);
});
