FROM nginx:latest

# Удаляем симлинки и создаем обычные файлы
RUN rm -f /var/log/nginx/access.log /var/log/nginx/error.log && \
    touch /var/log/nginx/access.log /var/log/nginx/error.log && \
    chmod 666 /var/log/nginx/access.log /var/log/nginx/error.log

# Копируем твой конфиг
COPY nginx.conf /etc/nginx/nginx.conf
