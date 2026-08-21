import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // 开发模式:控制台请求代理到 Node 管理 API
      "/api": "http://127.0.0.1:9090",
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
  },
});
