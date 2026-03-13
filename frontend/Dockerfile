FROM node:22-bookworm-slim AS build
WORKDIR /app

COPY frontend/package.json ./
COPY frontend/package-lock.json ./
COPY frontend/tsconfig.json ./
COPY frontend/tsconfig.app.json ./
COPY frontend/tsconfig.node.json ./
COPY frontend/vite.config.ts ./
COPY frontend/tailwind.config.ts ./
COPY frontend/postcss.config.js ./
COPY frontend/index.html ./
COPY frontend/public ./public
RUN npm ci

COPY frontend/src ./src
RUN npm run build

FROM nginx:1.27-alpine
RUN apk add --no-cache jq
COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY frontend/docker-entrypoint.d/40-runtime-config.sh /docker-entrypoint.d/40-runtime-config.sh
RUN chmod +x /docker-entrypoint.d/40-runtime-config.sh
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
