#!/bin/bash

set -e
cd ~

# you need like 1.5gb ram to install rust or something, so this makes a swap file to use that if its necessary. this will not persist on restart.
sudo dd if=/dev/zero of=/swapfile bs=1024 count=1048576
sudo mkswap /swapfile
sudo chmod 600 /swapfile
sudo swapon /swapfile


#echo to simulate an enter hit
sudo apt-get update
sudo curl --proto '=https' --tlsv1.2 https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"

#c compilers (required by a few libraries), openssl stuff, and nginx
sudo apt install -y build-essential libssl-dev pkg-config nginx

echo | sudo curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
nvm install 22

npm install pm2 -g

read -p "Enter the domain or IP for your server (e.g., 240.100.102.199 or urlhere.com): " domain

sudo tee /etc/nginx/conf.d/default.conf > /dev/null <<EOF
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

sudo systemctl restart nginx

cd RustHypixelApi

echo "Installation complete! See readme for usage instructions."