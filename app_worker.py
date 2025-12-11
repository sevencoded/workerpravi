# app_worker.py
import os
import time
import traceback

from utils import supabase, upload_file
from enf import extract_enf
from audio_fp import extract_audio_fingerprint
from phash import extract_video_phash

TMP_PATH = "/tmp/forensics_video.mp4"

def worker_loop():
    print("WORKER STARTED")

    while True:
        try:
            job_res = (
                supabase.table("forensic_queue")
                .select("*")
                .eq("status", "pending")
                .limit(1)
                .execute()
            )

            if not job_res.data:
                time.sleep(1)
                continue

            job = job_res.data[0]

            qid = job["id"]
            proof_id = job["proof_id"]
            user_id = job["user_id"]
            video_bytes = job["video_data"]

            # snimamo privremeni fajl
            with open(TMP_PATH, "wb") as f:
                f.write(video_bytes)

            supabase.table("forensic_queue").update({
                "status": "processing"
            }).eq("id", qid).execute()

            try:
                enf_hash, enf_png = extract_enf(TMP_PATH)
                audio_fp = extract_audio_fingerprint(TMP_PATH)
                video_phash = extract_video_phash(TMP_PATH)

                upload_file(f"{user_id}/{proof_id}_enf.png", enf_png, "image/png")

                supabase.table("forensic_results").insert({
                    "proof_id": proof_id,
                    "enf_hash": enf_hash,
                    "audio_fingerprint": audio_fp,
                    "video_phash": video_phash
                }).execute()

                # obriši temp fajl i video iz baze
                os.remove(TMP_PATH)
                supabase.table("forensic_queue").update({
                    "status": "completed",
                    "video_data": None
                }).eq("id", qid).execute()

            except Exception as e:
                print("PROCESS ERROR:", e)
                traceback.print_exc()
                supabase.table("forensic_queue").update({
                    "status": "failed"
                }).eq("id", qid).execute()

        except Exception as e:
            print("WORKER LOOP ERROR:", e)
            traceback.print_exc()

        time.sleep(1)

if __name__ == "__main__":
    worker_loop()
