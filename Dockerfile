FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PORT=3000

RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-encode a tiny real moving H.264 stream (320x180 @ 5 FPS, ~30kbps, 0% runtime FFmpeg CPU)
RUN ffmpeg -y -f lavfi -i testsrc=size=320x180:rate=5 -t 6 \
    -c:v libx264 -preset ultrafast -tune zerolatency \
    -profile:v baseline -level 3.1 -pix_fmt yuv420p \
    -b:v 30k -maxrate 40k -bufsize 40k \
    -g 5 -keyint_min 5 -sc_threshold 0 -bf 0 \
    -x264-params "slices=1:repeat-headers=1" \
    -f h264 /app/stream_loop.h264

COPY . .

EXPOSE 3000

CMD ["python", "-u", "main.py"]
