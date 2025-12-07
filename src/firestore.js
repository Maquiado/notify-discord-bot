import admin from 'firebase-admin';
if (!admin.apps.length) {
  const projectId = process.env.FIREBASE_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT || undefined;
  let credential;
  try { credential = admin.credential.applicationDefault(); } catch (_) { credential = undefined; }
  admin.initializeApp({ credential, projectId });
}
export const db = admin.firestore();
