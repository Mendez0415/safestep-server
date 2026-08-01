require('dotenv').config();
const express = require('express');
const mqtt = require('mqtt');
const { Expo } = require('expo-server-sdk');

const app = express();
app.use(express.json());

const expo = new Expo();

// ── Tokens guardados en memoria ───────────────────────────────────
let pushTokens = new Set();

// ── Conexión MQTT ─────────────────────────────────────────────────
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
    // Guardar token del celular
    if (Expo.isExpoPushToken(msg)) {
      pushTokens.add(msg);
      console.log(`Token guardado: ${msg}`);
      console.log(`Total tokens: ${pushTokens.size}`);
    }
  }

  if (topic === 'baston/estado' && msg === 'OFFLINE') {
    await sendPushNotification(
      '⚠️ Dispositivo desconectado',
      'SafeStep perdió la conexión WiFi'
    );
  }

  if (topic === 'baston/caida') {
    await sendPushNotification(
      '🚨 ¡Caída detectada!',
      'SafeStep detectó una posible caída'
    );
  }
});

mqttClient.on('error', (err) => {
  console.error('Error MQTT:', err.message);
});

// ── Mandar notificación push a todos los tokens ───────────────────
async function sendPushNotification(title, body) {
  if (pushTokens.size === 0) {
    console.log('No hay tokens registrados');
    return;
  }

  const messages = [];
  for (const token of pushTokens) {
    if (!Expo.isExpoPushToken(token)) continue;
    messages.push({
      to: token,
      sound: 'default',
      title,
      body,
      priority: 'high',
    });
  }

  try {
    const chunks = expo.chunkPushNotifications(messages);
    for (const chunk of chunks) {
      const receipts = await expo.sendPushNotificationsAsync(chunk);
      console.log('Notificaciones enviadas:', receipts);
    }
  } catch (error) {
    console.error('Error enviando notificaciones:', error);
  }
}

// ── Webhook de EMQX (respaldo) ────────────────────────────────────
app.post('/webhook', async (req, res) => {
  const { topic, payload } = req.body;
  console.log(`Webhook recibido [${topic}]: ${payload}`);

  if (topic === 'baston/estado' && payload === 'OFFLINE') {
    await sendPushNotification(
      '⚠️ Dispositivo desconectado',
      'SafeStep perdió la conexión WiFi'
    );
  }

  if (topic === 'baston/caida') {
    await sendPushNotification(
      '🚨 ¡Caída detectada!',
      'SafeStep detectó una posible caída'
    );
  }

  res.json({ ok: true });
});

// ── Ruta para ver tokens guardados ────────────────────────────────
app.get('/tokens', (req, res) => {
  res.json({ tokens: [...pushTokens], total: pushTokens.size });
});

// ── Ruta de prueba ────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({ status: 'SafeStep Server corriendo', tokens: pushTokens.size });
});

// ── Iniciar servidor ──────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`✓ Servidor corriendo en puerto ${PORT}`);
});
