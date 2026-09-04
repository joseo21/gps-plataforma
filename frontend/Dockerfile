# Build del frontend Vue desde github.com/Sebastian1841/GpsSinergy
# Cloná el repo dentro de ./frontend/src-repo o ajustá el contexto.
FROM node:22-alpine AS build
WORKDIR /build
COPY src-repo/frontend/package*.json ./
RUN npm ci
COPY src-repo/frontend/ ./
RUN npm run build

FROM nginx:alpine
COPY --from=build /build/dist /usr/share/nginx/html
COPY spa.conf /etc/nginx/conf.d/default.conf
