import admin from 'firebase-admin';
if (!admin.apps.length) {
  const projectId = process.env.FIREBASE_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT || 'custom-das-estrelas';
  let credential;
  try { credential = admin.credential.applicationDefault(); } catch (_) { credential = undefined; }
  const saJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if (!credential && saJson) {
    try { credential = admin.credential.cert(JSON.parse(saJson)); } catch (_) {}
  }
  if (!credential && process.env.FIREBASE_CLIENT_EMAIL && process.env.FIREBASE_PRIVATE_KEY) {
    const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
    let privateKey = process.env.FIREBASE_PRIVATE_KEY;
    if (privateKey && privateKey.startsWith('"') && privateKey.endsWith('"')) privateKey = privateKey.slice(1, -1);
    privateKey = privateKey.replace(/\\n/g, '\n');
    credential = admin.credential.cert({ projectId, clientEmail, privateKey });
  }
  admin.initializeApp({ credential, projectId });
}
export const db = admin.firestore();
