import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  // REVIEW §4.15:相对 base——构建产物可在站点根或任意子路径挂载
  // (此前绝对 "/",仅站点根可用;hash 路由不受影响)。
  base: "./",
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
