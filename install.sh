#!/bin/bash

set -e
cd ~

# you need like 1.5gb ram to install rust or something, so this makes a swap file to use that if its necessary. this will not persist on restart.
dd if=/dev/zero of=/swapfile bs=1024 count=1048576
mkswap /swapfile
chmod 600 /swapfile
swapon /swapfile


#echo to simulate an enter hit
echo | curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh
source "$HOME/.cargo/env"

#c compilers (required by a few libraries)
echo | apt update
echo | apt install build-essential

echo | apt install libssl-dev
echo | apt install pkg-config

echo | curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash
source "$HOME/.nvm/nvm.sh"
nvm install 22

npm install pm2 -g

echo | apt install nginx
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