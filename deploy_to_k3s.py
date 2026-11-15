import sys
import os
import subprocess
import time
import paramiko
from scp import SCPClient

tag = sys.argv[1] if len(sys.argv) > 1 else "1.0"
image_name = f"riot-server:{tag}"
print(f"Deploying {image_name} to k3s...", flush=True)

local_tar = os.path.abspath(f"D:/labs/sem7/riot/.temp/riot-server-{tag}.tar")
print(f"Saving docker image {image_name} -> {local_tar}...", flush=True)
subprocess.run(["docker", "save", "-o", local_tar, image_name], check=True)

host = "192.168.0.193"
user = "nluloo"
pwd = "nluloo"
remote_tar = f"/home/nluloo/riot-server-{tag}.tar"

print(f"Uploading to {host}...", flush=True)
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(host, port=22, username=user, password=pwd, timeout=10)

with SCPClient(ssh.get_transport()) as scp:
    scp.put(local_tar, remote_tar)

print("Importing into k3s ctr...", flush=True)
cmd = f"echo {pwd} | sudo -S k3s ctr images import {remote_tar}"
stdin, stdout, stderr = ssh.exec_command(cmd)
out = stdout.read().decode('utf-8')
err = stderr.read().decode('utf-8')
print("Import output:\n", out, flush=True)
if err:
    print("Import stderr:\n", err, flush=True)

ssh.close()

if os.path.exists(local_tar):
    os.remove(local_tar)

kubeconfig = os.environ.get("KUBECONFIG", r"D:\labs\sem7\riot\.temp\k3s_direct.yaml")
print(f"Updating deployment riot-server to {image_name} using {kubeconfig}...", flush=True)
subprocess.run([
    "kubectl", "--kubeconfig", kubeconfig,
    "set", "image", "deployment/riot-server", f"server={image_name}"
], check=True)

print("Waiting for rollout status...", flush=True)
subprocess.run([
    "kubectl", "--kubeconfig", kubeconfig,
    "rollout", "status", "deployment/riot-server"
], check=True)

print("Deployment successfully updated!", flush=True)
