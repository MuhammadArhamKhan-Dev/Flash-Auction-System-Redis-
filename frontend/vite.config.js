import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In development, /api and /ws are forwarded to backend 1.
const backend = "http://127.0.0.1:8001";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": backend,
      "/ws": { target: backend, ws: true },
    },
  },
});
