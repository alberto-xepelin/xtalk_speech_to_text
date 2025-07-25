from google.cloud import storage
from pydub import AudioSegment
from google.cloud import speech
import os
import glob
import textwrap
import subprocess
import json


def convert_to_wav_if_needed(filepath):
    try:
        result = subprocess.run(["file", filepath], capture_output=True, text=True)
        if "WAV" not in result.stdout:
            print("Archivo no es WAV real. Convirtiendo...")
            fixed_path = filepath.replace(".wav", "_fixed.wav")
            subprocess.run([
                "ffmpeg", "-y", "-i", filepath,
                "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "2",
                fixed_path
            ], check=True)
            return fixed_path
        else:
            return filepath
    except Exception as e:
        raise RuntimeError(f"❌ Error al convertir archivo: {e}")


def read_audio_file(pais, nombre_file, tmp_dir):
    
    bucket_name = "xepelin-ds-prod-xtalk"
    blob_path = f"audios/{pais}/{nombre_file}"
    local_path = f"{tmp_dir}/{nombre_file}"

    try:
        # 1. Descargar desde el bucket original
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.download_to_filename(local_path)
        print(f"✅ Archivo descargado en: {local_path}")

        # 2. Convertir si es necesario
        final_path = convert_to_wav_if_needed(local_path)

        # 3. Subir archivo final (posiblemente convertido) a xtalk_logs_v1
        try:
            copy_bucket = storage.Client().bucket("xtalk_logs_v1")
            carpeta_file = nombre_file.split('.wav')[0]
            nombre_final = os.path.basename(final_path)
            blob_copy_path = f"{pais}/{carpeta_file}/original/{nombre_final}"
            copy_bucket.blob(blob_copy_path).upload_from_filename(final_path)
            print(f"☁️ Archivo final subido a: gs://xtalk_logs_v1/{blob_copy_path}")
        except Exception as e:
            print(f"⚠️ No se pudo subir el archivo final a GCS: {str(e)}")

        return final_path, 200 

    except Exception as e:
        return f"❌ Error al leer archivo desde GCS: {str(e)}", 500


def separar_canales(audio_path, carpeta_file, pais, tmp_dir):

    try:
        base_name = os.path.splitext(os.path.basename(audio_path))[0]
        left_wav = f"{tmp_dir}/{base_name}_left.wav"
        right_wav = f"{tmp_dir}/{base_name}_right.wav"

        # 1. Separar los canales
        audio = AudioSegment.from_wav(audio_path)
        channels = audio.split_to_mono()
        channels[0].export(left_wav, format="wav")
        channels[1].export(right_wav, format="wav")
        print(f"✅ Canales separados: {left_wav}, {right_wav}")

        # 2. Mergear los canales (Monocanal)
        merged_wav = f"{tmp_dir}/{base_name}_merged.wav"
        audio_mergeado = channels[0].overlay(channels[1])
        audio_mergeado.export(merged_wav, format="wav")
        print(f"🔁 Canales mergeados en: {merged_wav}")
        
        # 3. Subir todos los archivos GCS
        try:
            bucket_name = "xtalk_logs_v1"
            client = storage.Client()
            bucket = client.bucket(bucket_name)

            left_wav_blob = f"{pais}/{carpeta_file}/left/{base_name}_left.wav"
            right_wav_blob = f"{pais}/{carpeta_file}/right/{base_name}_right.wav"
            merged_wav_blob = f"{pais}/{carpeta_file}/merged/{base_name}_merged.wav"

            # Subir canales
            bucket.blob(left_wav_blob).upload_from_filename(left_wav)
            print(f"☁️ Subido canal izquierdo WAV a gs://{bucket_name}/{left_wav_blob}")

            bucket.blob(right_wav_blob).upload_from_filename(right_wav)
            print(f"☁️ Subido canal derecho WAV a gs://{bucket_name}/{right_wav_blob}")

            # Subir mergeado
            bucket.blob(merged_wav_blob).upload_from_filename(merged_wav)
            print(f"☁️ Subido audio mergeado a gs://{bucket_name}/{merged_wav_blob}")

            return (left_wav_blob, right_wav_blob, left_wav, right_wav, merged_wav), 200
        
        except Exception as e:
            return f"❌ Error al intentar subir archivos a GCS: {str(e)}", 500
    
    except Exception as e:
        return f'❌ Error al separar canales {str(e)}', 500


def segmentar_audio(left_wav, right_wav, merged_wav):

    try:
        base_dir = os.path.dirname(left_wav)

        # 1. Crear carpetas de salida para los segmentos
        left_seg_dir = os.path.join(base_dir, "segmentos_left")
        right_seg_dir = os.path.join(base_dir, "segmentos_right")
        merged_seg_dir = os.path.join(base_dir, "segmentos_merged")

        os.makedirs(left_seg_dir, exist_ok=True)
        os.makedirs(right_seg_dir, exist_ok=True)
        os.makedirs(merged_seg_dir, exist_ok=True)

        # 2. Definir los patrones de salida
        output_pattern_left = os.path.join(left_seg_dir, "_parte_%03d.wav")
        output_pattern_right = os.path.join(right_seg_dir, "_parte_%03d.wav")
        output_pattern_merged = os.path.join(merged_seg_dir, "_parte_%03d.wav")

        # 3. Ejecutar segmentación con ffmpeg
        subprocess.run([
            "ffmpeg", "-i", left_wav,
            "-f", "segment", "-segment_time", "540",
            "-c", "copy", output_pattern_left
        ], check=True)

        subprocess.run([
            "ffmpeg", "-i", right_wav,
            "-f", "segment", "-segment_time", "540",
            "-c", "copy", output_pattern_right
        ], check=True)

        subprocess.run([
                "ffmpeg", "-i", merged_wav,
                "-f", "segment", "-segment_time", "540",
                "-c", "copy", output_pattern_merged
            ], check=True)

        return (output_pattern_left, output_pattern_right, output_pattern_merged), 200

    except Exception as e:
        return f'❌ Error al segmentar audio {str(e)}', 500


def subir_segmentos_a_gcs(dir_local, bucket_name, carpeta_destino_gcs):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        archivos = sorted(glob.glob(os.path.join(dir_local, "*.wav")))

        for local_path in archivos:
            nombre_archivo = os.path.basename(local_path)
            blob_path = f"{carpeta_destino_gcs}/{nombre_archivo}"
            bucket.blob(blob_path).upload_from_filename(local_path)
            print(f"☁️ Subido a gs://{bucket_name}/{blob_path}")

        return archivos, 200

    except Exception as e:
        return f"❌ Error al subir segmentos a GCS: {str(e)}", 500


def transcribir_segmentos(openai_client, lista_segmentos, canal, output_txt):
    try:
        with open(output_txt, "w", encoding="utf-8") as f_out:

            for local_path in lista_segmentos:
                with open(local_path, "rb") as audio_file:
                    response = openai_client.audio.transcriptions.create(
                        model="whisper-1",
                        file=audio_file,
                        language="es",
                        response_format="verbose_json"
                    )

                for segment in response.segments:
                    text = segment.text.strip()
                    start, end = segment.start, segment.end

                    linea = f"{round(start, 2)}:{round(end, 2)} | {text}\n"

                    if text.strip() != "":
                        
                        f_out.write(linea)

        print(f"✅ Transcripción del canal {canal} guardada en: {output_txt}")
        return output_txt, 200

    except Exception as e:
        return f"❌ Error en transcripción del canal {canal}: {str(e)}", 500


def subir_transcripcion_a_gcs(local_path_txt, bucket_name, blob_path_txt):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path_txt)

        blob.upload_from_filename(local_path_txt)
        print(f"📄 Transcripción subida a gs://{bucket_name}/{blob_path_txt}")

        return f"Transcripción subida correctamente a gs://{bucket_name}/{blob_path_txt}", 200

    except Exception as e:
        return f"❌ Error al subir transcripción: {str(e)}", 500


def generar_dialogo_final(openai_client, contenido_merged, output_path_txt):
    try:
        prompt_dialogo = textwrap.dedent(f"""
        Estoy transcribiendo una llamada entre un ejecutivo de Xepelin (sdr) y un cliente (client).

        La transcripción completa de la llamada es la siguiente:

        {contenido_merged}

        Necesito que reconstruyas el diálogo intercalado en el orden correcto, asignando correctamente quién habla en cada turno.

        Intrucciones importantes:
        - El emisor es el 'sdr' y el receptor el 'client'.
        - El `sdr` es el ejecutivo que realiza la llamada para consultar sobre facturas u ofrecer algun servicio financiero.
        - Cuando habla una grabadora de inmediato corresponde a 'client' (pues se esta llamando al cliente y contesta su grabadora).
        - El 'client' es la persona a quien se le ofrecen productos o se le preguntan por facturas.


        Además necesito que me hagas un resumen de la llamada. Quiero que el output sea en el siguiente formato:

        ```json
        {{
        "transcription": [
            {{
            "role": "sdr",
            "content": "Hola, muy buenos días. Disculpe, ¿me podría por favor comunicar con la contadora Reyes?"
            }},
            {{
            "role": "client",
            "content": "¿De dónde llamas?"
            }},
            {{
            "role": "sdr",
            "content": "De Zeppelin."
            }}
        ],
        "summary": "Un SDR de Zeppelin contacta a la contadora Reyes para presentarle una propuesta de línea de crédito empresarial. La contadora confirma haber recibido la información y acuerdan un seguimiento para la semana siguiente."
        }}
        No expliques nada adicional. Solo devuelve el diccionario en el formato indicado.
        """)

        response = openai_client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Eres un asistente útil que transforma transcripciones en diálogos claros."},
                {"role": "user", "content": prompt_dialogo}
            ],
            temperature=0.3
        )

        output_raw = response.choices[0].message.content.strip()

        default_dict = {}
        default_dict['transcription'] = [
            {'role': 'client', 'content': 'no transcription'},
            {'role': 'sdr', 'content': 'no transcription'}
        ]
        default_dict['summary'] = 'no transcription'

        try:
            output_raw = output_raw.replace("```json", "").replace("```", "").strip()
            output_dict = json.loads(output_raw)
        except:
            output_dict = default_dict

        with open(output_path_txt, 'w', encoding='utf-8') as f:
            json.dump(output_dict, f, ensure_ascii=False, indent=4)

        print(f"📄 Diálogo generado y guardado en: {output_path_txt}")
        return output_dict, 200

    except Exception as e:
        return f"❌ Error al generar diálogo final con GPT: {str(e)}", 500

def leer_contenido_archivo(ruta_archivo):
    try:
        with open(ruta_archivo, "r", encoding="utf-8") as f:
            return f.read(), 200
    except Exception as e:
        return f"❌ Error al leer archivo {ruta_archivo}: {str(e)}", 500

def subir_transcripcion_a_gcs_json(local_path_txt, bucket_name, blob_path_txt):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path_txt)

        blob.upload_from_filename(local_path_txt, content_type="application/json")
        print(f"📄 Transcripción subida a gs://{bucket_name}/{blob_path_txt}")

        return f"Transcripción subida correctamente a gs://{bucket_name}/{blob_path_txt}", 200

    except Exception as e:
        return f"❌ Error al subir transcripción: {str(e)}", 500