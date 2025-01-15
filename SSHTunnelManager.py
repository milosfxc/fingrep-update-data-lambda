from sshtunnel import SSHTunnelForwarder
from config import BASTION_IP, BASTION_PORT, BASTION_USER, BASTION_KEY, REMOTE_DB_HOST, DB_PORT
import config


class SSHTunnelManager:
    def __init__(self):
        self.tunnel = SSHTunnelForwarder(
            (BASTION_IP, BASTION_PORT),
            ssh_username=BASTION_USER,
            ssh_pkey=BASTION_KEY,
            remote_bind_address=(REMOTE_DB_HOST, DB_PORT)
        )

    def __enter__(self):
        self.tunnel.start()
        config.LOCAL_BIND_PORT = self.tunnel.local_bind_port
        return self.tunnel

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tunnel.stop()