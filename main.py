from flask import Flask, request
from google.cloud import storage
import os
import uuid
import openai
import glob
from io import StringIO
from functions import (
    read_audio_file, 
    separar_canales, 
    segmentar_audio,
    subir_segmentos_a_gcs,
    transcribir_segmentos,
    subir_transcripcion_a_gcs,
    generar_dialogo_final,
    leer_contenido_archivo,
    subir_transcripcion_a_gcs_json
)
from dotenv import load_dotenv
import json

load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")

TOKEN_HSP_API = os.getenv("TOKEN_HSP_API")

app = Flask(__name__)

print("✔ Flask app is loading...")

@app.route("/", methods=["POST"])
def pipeline():
    pais = request.args.get("country", "MX")
    url_audio = request.args.get("url_audio", "-")

    nombre_file = f"audio_{url_audio.split('/')[-1]}.wav"
    carpeta_file = nombre_file.split('.wav')[0]

    # 👉 Nueva carpeta temporal única
    unique_id = str(uuid.uuid4())
    tmp_dir = f"/tmp/{unique_id}"
    os.makedirs(tmp_dir, exist_ok=True)

    # Data Pipeline

    # i. Chequear si existe el archivo
    client = storage.Client()
    bucket = client.bucket("xepelin-ds-prod-xtalk")
    blob_path = f"audios/{pais}/{carpeta_file}/{nombre_file}"
    blob = bucket.blob(blob_path)

    print('BLOB PATH:', blob_path)

    if blob.exists(client):
        print('EXISTENCIA: EXISTEEEE')
    else:
        print('EXISTENCIA: NO EXISTEEEE')

    # 0. Chequear si ya existe la transcripción
    client = storage.Client()
    bucket = client.bucket("xtalk_logs_v1")
    blob_path = f"{pais}/{carpeta_file}/transcript_diarizacion.json"
    blob = bucket.blob(blob_path)

    if blob.exists(client):
        content = blob.download_as_text()
        content_dict = json.loads(content)
        return content_dict, 200

    # 1. Leer el archivo de audio
    final_path, signal_1 = read_audio_file(pais, nombre_file, tmp_dir)

    if signal_1 != 200:
        return final_path, signal_1
    
    # 2. Separar los canales
    all_paths, signal_2 = separar_canales(final_path, carpeta_file, pais, tmp_dir)

    if signal_2 != 200:
        return all_paths, signal_2
    
    left_wav = all_paths[2]
    right_wav = all_paths[3]
    merged_wav = all_paths[4]

    # 3. Segmentar ambos canales
    segment_output, signal_3 = segmentar_audio(left_wav, right_wav, merged_wav)

    if signal_3 != 200:
        return segment_output, signal_3
    
    output_pattern_left, output_pattern_right, output_pattern_merged = segment_output

    left_seg_dir = os.path.dirname(output_pattern_left)
    right_seg_dir = os.path.dirname(output_pattern_right)
    merged_seg_dir = os.path.dirname(output_pattern_merged)

    # 4. Subir segmentos a GCS
    gcs_prefix = f"{pais}/{carpeta_file}"

    segs_left, signal_4a = subir_segmentos_a_gcs(
        left_seg_dir, "xtalk_logs_v1", f"{gcs_prefix}/left_segmentos"
    )

    if signal_4a != 200:
        return segs_left, signal_4a

    segs_right, signal_4b = subir_segmentos_a_gcs(
        right_seg_dir, "xtalk_logs_v1", f"{gcs_prefix}/right_segmentos"
    )

    if signal_4b != 200:
        return segs_right, signal_4b
    
    segs_merged, signal_4c = subir_segmentos_a_gcs(
        merged_seg_dir, "xtalk_logs_v1", f"{gcs_prefix}/merged_segmentos"
    )

    if signal_4c != 200:
        return segs_merged, signal_4c
    
    # 5. Transcribir segmentos con OpenAI Whisper
    openai_client = openai.OpenAI()

    path_txt_merged = f"{tmp_dir}/transcripcion_por_palabra_merged.txt"

    transcr_merged, signal_5c = transcribir_segmentos(
        openai_client=openai_client,
        lista_segmentos=sorted(glob.glob(os.path.join(merged_seg_dir, "*.wav"))),
        canal="merged",
        output_txt=path_txt_merged
    )

    if signal_5c != 200:
        return transcr_merged, signal_5c
    
    # 6. Subir transcripciones
    msg_merged_upload, signal_6c = subir_transcripcion_a_gcs(
        local_path_txt=path_txt_merged,
        bucket_name="xtalk_logs_v1",
        blob_path_txt=f"{gcs_prefix}/transcripcion_por_palabra_merged.txt"
        )
    
    if signal_6c != 200:
        return msg_merged_upload, signal_6c
    
    # 7. Leer transcripciones
    contenido_merged, signal_7c = leer_contenido_archivo(path_txt_merged)
    if signal_7c != 200:
        return contenido_merged, signal_7c

    # 8. Transcripcion final
    path_dialogo_txt = f"{tmp_dir}/transcript_diarizacion.json"

    dialogo, signal_8a = generar_dialogo_final(
        openai_client=openai_client,
        contenido_merged=contenido_merged,
        output_path_txt=path_dialogo_txt
    )

    if signal_8a != 200:
        return dialogo, signal_8a
    
    # 9. Subir JSON final a GCS.
    msg_json_upload, signal_9 = subir_transcripcion_a_gcs_json(
        local_path_txt=path_dialogo_txt,
        bucket_name="xtalk_logs_v1",
        blob_path_txt=f"{gcs_prefix}/transcript_diarizacion.json"
    )

    if signal_9 != 200:
        return msg_json_upload, signal_9

    return dialogo, 200