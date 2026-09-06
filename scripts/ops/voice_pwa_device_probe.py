#!/usr/bin/env python3
"""Read-only Android preflight for the KenigEvents voice preview. Never logs tokens."""
from __future__ import annotations
import argparse,json,re,shutil,subprocess
from urllib.parse import urlsplit

def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--serial',required=True,help='Explicit authorized ADB device; not written to the report')
    parser.add_argument('--preview-url',required=True)
    args=parser.parse_args()
    url=urlsplit(args.preview_url)
    if url.scheme!='https' or url.hostname!='kenigevents.ru' or not url.path.startswith('/preview-') or url.query or url.fragment:
        parser.error('Expected existing HTTPS kenigevents.ru/preview-…/ path, without query credentials')
    adb=shutil.which('adb')
    if not adb: parser.error('ADB is not installed in this environment')
    if not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}',args.serial): parser.error('Invalid device selector')
    def read(*command: str) -> str:
        result=subprocess.run([adb,'-s',args.serial,*command],capture_output=True,text=True,timeout=15,check=False)
        if result.returncode: raise RuntimeError('adb_read_failed; authorize/check the selected device without resetting it')
        return result.stdout.strip()
    if read('get-state')!='device': raise RuntimeError('selected_device_not_ready')
    raw=read('shell','dumpsys','package','com.android.chrome')
    version=re.search(r'\bversionName=([^\s]+)',raw)
    sockets=read('shell','cat','/proc/net/unix')
    result={'contract':'kenigevents.voice-phone-preflight.v1','read_only':True,'device_ready':True,
      'android_release':read('shell','getprop','ro.build.version.release'),
      'chrome_version':version.group(1) if version else None,
      'chrome_devtools_socket_present':'chrome_devtools_remote' in sockets,
      'preview_url':args.preview_url,'asr_tested':False,'pwa_tested':False,
      'state_changed':False}
    print(json.dumps(result,ensure_ascii=False,indent=2))
if __name__=='__main__':
    try: main()
    except (RuntimeError,subprocess.TimeoutExpired) as exc: raise SystemExit(str(exc)) from None
