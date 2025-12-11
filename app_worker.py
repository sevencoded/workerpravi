# app_worker.py
import os
import time
import traceback

from utils import supabase
from enf import extract_enf
from audio_fp import extract_audio_fingerprint
from phash import extract_video_phash

TMP_PATH = "/tmp/forensics_video.mp4"


def download_from_storage(path: str) -> bytes:
    res = supabase.storage.from_("main_videos").download(path)
    return res


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
            video_path = job["video_path"]  # NOVO!

            # ➤ Skini slice iz storage
            video_bytes = download_from_storage(video_path)

            # Snimi ga u temp file
            with open(TMP_PATH, "wb") as f:
                f.write(video_bytes)

            # Markiraj kao processing
            supabase.table("forensic_queue").update({
                "status": "processing"
            }).eq("id", qid).execute()

            try:
                # Forenzička obrada
                enf_hash, enf_png = extract_enf(TMP_PATH)
                audio_fp = extract_audio_fingerprint(TMP_PATH)
                video_phash = extract_video_phash(TMP_PATH)

                # Spremi rezultate u bazu
                supabase.table("forensic_results").insert({
                    "proof_id": proof_id,
                    "enf_hash": enf_hash,
                    "audio_fingerprint": audio_fp,
                    "video_phash": video_phash
                }).execute()

                # obriši temp fajl
                if os.path.exists(TMP_PATH):
                    os.remove(TMP_PATH)

                supabase.table("forensic_queue").update({
                    "status": "completed"
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
