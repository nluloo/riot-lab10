import sys
import time
import requests

BASE_URL = "http://localhost:8085"
JOB_NAME = "RIOT-CICD"

session = requests.Session()

def get_crumb():
    try:
        r = session.get(f"{BASE_URL}/crumbIssuer/api/json")
        if r.status_code == 200:
            data = r.json()
            return {data["crumbRequestField"]: data["crumb"]}
    except Exception:
        pass
    return {}

def trigger_build():
    headers = get_crumb()
    r = session.post(f"{BASE_URL}/job/{JOB_NAME}/build", headers=headers)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Failed to trigger build: {r.status_code} {r.text}")
    print(f"Triggered build for {JOB_NAME} (status {r.status_code})")
    queue_url = r.headers.get("Location")
    return queue_url

def wait_for_build(prev_build_num=0, timeout=180):
    start = time.time()
    build_num = None
    print("Waiting for build to appear...", flush=True)
    while time.time() - start < timeout:
        r = session.get(f"{BASE_URL}/job/{JOB_NAME}/api/json")
        data = r.json()
        last_build = data.get("lastBuild")
        if last_build and last_build["number"] > prev_build_num:
            build_num = last_build["number"]
            break
        time.sleep(1)

    if not build_num:
        raise TimeoutError("Build did not start within timeout")

    print(f"Build #{build_num} started, waiting for completion...", flush=True)
    while time.time() - start < timeout:
        r = session.get(f"{BASE_URL}/job/{JOB_NAME}/{build_num}/api/json")
        bdata = r.json()
        if not bdata.get("building"):
            result = bdata.get("result")
            print(f"Build #{build_num} finished with result: {result}")
            return build_num, result
        time.sleep(2)

    raise TimeoutError(f"Build #{build_num} did not complete within timeout")

def get_console(build_num):
    r = session.get(f"{BASE_URL}/job/{JOB_NAME}/{build_num}/consoleText")
    return r.text

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    if cmd == "build":
        r = session.get(f"{BASE_URL}/job/{JOB_NAME}/api/json")
        prev = r.json().get("lastBuild", {})
        prev_num = prev.get("number", 0) if prev else 0
        trigger_build()
        bnum, res = wait_for_build(prev_num)
        print("=== CONSOLE OUTPUT ===")
        print(get_console(bnum))
        sys.exit(0 if res == "SUCCESS" else 1)
    elif cmd == "log":
        bnum = sys.argv[2]
        print(get_console(bnum))
