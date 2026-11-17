import socket
import json
import time
import subprocess
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
kubeconfig = os.environ.get("KUBECONFIG", os.path.join(os.path.dirname(script_dir), "k3s-config", "k3s.yaml"))
pf_proc = subprocess.Popen([
    "kubectl", "--kubeconfig", kubeconfig,
    "port-forward", "svc/riot-server", "5000:5000"
], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

time.sleep(2)

def send_req(op, data={}):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", 5000))
    msg = json.dumps({"operation": op, "data": data}) + "\n"
    s.sendall(msg.encode('utf-8'))
    buf = b""
    while not buf.endswith(b"\n"):
        chunk = s.recv(4096)
        if not chunk:
            break
        buf += chunk
    s.close()
    return json.loads(buf.decode('utf-8').strip())

try:
    print("=== 1. Поиск свободных авто (SearchCar) ===")
    res1 = send_req("SearchCar")
    print(json.dumps(res1, ensure_ascii=False, indent=2))

    print("\n=== 2. Создание аренды (CreateRental) ===")
    res2 = send_req("CreateRental", {
        "fullName": "Иванов Иван Иванович",
        "phone": "+375291234567",
        "registrationNumber": "1234 AB-7",
        "days": 3,
        "startDate": "2026-11-15T12:00:00"
    })
    print(json.dumps(res2, ensure_ascii=False, indent=2))

    rental_id = 1
    if res2.get("data") and isinstance(res2["data"], dict) and "id" in res2["data"]:
        rental_id = res2["data"]["id"]

    print(f"\n=== 3. Возврат автомобиля (ReturnCar, rentalId={rental_id}) ===")
    res3 = send_req("ReturnCar", {"rentalId": rental_id})
    print(json.dumps(res3, ensure_ascii=False, indent=2))

    print("\n=== 4. Проверка итогового состояния (ShowState) ===")
    res4 = send_req("ShowState")
    print(json.dumps(res4, ensure_ascii=False, indent=2))

finally:
    pf_proc.terminate()
