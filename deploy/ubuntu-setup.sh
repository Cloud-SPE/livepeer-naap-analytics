# Software installs
sudo apt update;apt upgrade -y
sudo apt install -y  ufw fail2ban jq net-tools ca-certificates curl


# INSTALL DOCKER ENGINE
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
docker ps

# configuration
hostnamectl set-hostname lpc-infra2

# user creation
sudo useradd -m -s /bin/bash dockerusr
sudo usermod -aG sudo dockerusr

sudo passwd dockerusr