FROM python:3.11-slim

# ffmpeg/ffprobe isolated in the container
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg fonts-dejavu-core fonts-noto-core fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY ./video .
COPY ./utils ./utils
RUN pip install --no-cache-dir -r ./utils/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download Whisper model during build to avoid runtime latency spikes
RUN python -c "from faster_whisper import WhisperModel; WhisperModel('small', device='cpu', compute_type='int8', download_root='/opt/whisper_models')"

CMD [ "python", "-u", "video.py" ]