#!/bin/bash

set -e
cd ~

# you need like 1.5gb ram to install rust or something, so this makes a swap file to use that if its necessary. this will not persist on restart.
dd if=/dev/zero of=/swapfile bs=1024 count=1048576
mkswap /swapfile
chmod 600 /swapfile
swapon /swapfile


#echo to simulate an enter hit
apt-get update
curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"

#c compilers (required by a few libraries), openssl stuff, and nginx
apt install -y build-essential libssl-dev pkg-config nginx

curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | sh -y
source "$HOME/.nvm/nvm.sh"
nvm install 22

npm install pm2 -g

read -p "Enter the domain or IP for your server (e.g., 240.100.102.199 or urlhere.com): " domain

cat <<'EOF' > /etc/nginx/conf.d/default.conf
server {
        listen 80;
        listen [::]:80;
        server_name $domain;

        location / {
                proxy_pass http://127.0.0.1:8000;
                proxy_set_header Host \$host;
                proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
                proxy_set_header X-Real-IP \$remote_addr;
        }
}
EOF

systemctl restart nginx

cd RustHypixelApi