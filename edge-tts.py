from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
import edge_tts
import asyncio
import os
import uuid

app = Flask(__name__)
CORS(app)  # 允许跨域

OUTPUT_DIR = "audio"
os.makedirs(OUTPUT_DIR, exist_ok=True)

@app.route("/api/ai/tts", methods=["POST"])
def tts():
    data = request.json
    text = data.get("text", "")
    if not text:
        return jsonify({"error": "No text provided"}), 400

    filename = f"{uuid.uuid4().hex}.mp3"
    filepath = os.path.join(OUTPUT_DIR, filename)

    asyncio.run(generate_tts(text, filepath))
    return jsonify({"url": f"https://redditdata.onrender.com/api/ai/audio/{filename}"})

async def generate_tts(text, path):
    communicate = edge_tts.Communicate(text, voice="zh-CN-XiaoxiaoNeural")
    await communicate.save(path)

@app.route("/api/ai/audio/<filename>")
def serve_audio(filename):
    path = os.path.join(OUTPUT_DIR, filename)
    if os.path.exists(path):
        return send_file(path, mimetype="audio/mpeg")
    else:
        return "File not found", 404

if __name__ == "__main__":
    app.run(debug=True)
