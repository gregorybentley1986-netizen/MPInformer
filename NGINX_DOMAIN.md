# Админка на домене mpi.laprinta.ru

Пошаговая инструкция для вывода MPInformer (админка + API) на домен.

## Предварительно

1. DNS: запись `mpi.laprinta.ru` должна указывать на IP вашего VPS (тип A).
2. Подождите 5–15 минут после изменения DNS.

## 1. Установка nginx и certbot (на VPS)

```bash
sudo apt update
sudo apt install -y nginx certbot python3-certbot-nginx
```

## 2. Копирование конфига

С компьютера (из папки проекта MPInformer):

```bash
scp nginx-mpi.laprinta.ru.conf root@IP_ВАШЕГО_VPS:/tmp/
```

Или на VPS создать файл вручную:

```bash
sudo nano /etc/nginx/sites-available/mpi.laprinta.ru
```

Вставьте содержимое из файла `nginx-mpi.laprinta.ru.conf` (из корня проекта).

## 3. Включение сайта

```bash
sudo ln -sf /etc/nginx/sites-available/mpi.laprinta.ru /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

## 4. Открыть порты (если используете ufw)

```bash
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw allow 22/tcp
sudo ufw enable
```

## 5. HTTPS (Let's Encrypt)

```bash
sudo certbot --nginx -d mpi.laprinta.ru
```

Certbot обновит конфиг и добавит SSL. Продлевать сертификат вручную не нужно — certbot сделает это автоматически.

## 6. Проверка

Откройте в браузере: **https://mpi.laprinta.ru**

Должна открыться админка MPInformer.

## 7. После перехода на HTTPS

- В файле на сервере `/etc/nginx/sites-available/mpi.laprinta.ru` можно включить редирект с HTTP на HTTPS: в блоке `server { listen 80; ... }` добавить первой строкой `return 301 https://$server_name$request_uri;` (и убрать или закомментировать `location /` в этом блоке, либо оставить один блок только для 443).
- В коде приложения (Python/шаблоны) ничего менять не нужно: ссылки относительные, камера в браузере будет доступна по HTTPS.
