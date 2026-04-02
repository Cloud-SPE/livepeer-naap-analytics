sudo ufw default deny incoming
sudo ufw default allow outgoing

# SSH
sudo ufw allow 9222/tcp
# Portainer Agent — restrict to Portainer server IP only (full Docker socket + /host mount)
# Replace <PORTAINER_SERVER_IP> with the static IP of the Portainer management server
sudo ufw allow from <PORTAINER_SERVER_IP> to any port 9001 proto tcp comment 'Portainer Agent - Portainer server only'
# Traefik HTTPS
sudo ufw allow 443
# Traefik metrics — restrict to ops/monitoring CIDR only (exposes internal service topology)
# Replace <OPS_CIDR> with your Prometheus server IP or ops bastion CIDR
sudo ufw allow from <OPS_CIDR> to any port 8443 proto tcp comment 'Traefik metrics - ops only'
# Kafka (external TLS via Traefik)
sudo ufw allow 9092
# ClickHouse has no Docker ports: mapping — no host rule needed

sudo ufw enable
sudo ufw status numbered
