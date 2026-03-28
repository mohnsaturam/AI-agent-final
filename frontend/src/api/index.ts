import { createClient } from './client';

// Dev token generated with secret: umsa-dev-secret-key-2026-do-not-use-in-prod
// payload: { "user_id": "fd78c5c5-c290-449e-b784-52432272b8a8", "user_role": "admin" }
// Expires: 2026-03-29 (30-day token)
const DEV_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiZmQ3OGM1YzUtYzI5MC00NDllLWI3ODQtNTI0MzIyNzJiOGE4IiwidXNlcl9yb2xlIjoiYWRtaW4iLCJleHAiOjE3NzQ3NjcwNzh9.QHJx6IckbaOz8XzwIMcsuVUiB8_8nNijZzR76I0WIcQ";

export const umsaApi = createClient(
    import.meta.env.VITE_API_URL || "http://localhost:8080",
    localStorage.getItem('umsa_token') || DEV_TOKEN
);
