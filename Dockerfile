FROM python:3.11-slim

RUN useradd --create-home --uid 1000 ubuntu
WORKDIR /home/ubuntu

# Copy requirements first as to not disturb cache for other changes.
COPY requirements.txt .

RUN pip3 install -r requirements.txt && \
  pip3 install gunicorn

# Fonts required for Pillow tag rendering
RUN apt-get update && apt-get install -y --no-install-recommends fonts-dejavu-core && \
  rm -rf /var/lib/apt/lists/*

USER ubuntu

# Finally, copy the entire source.
COPY --chown=ubuntu:ubuntu . .

ENV FLASK_APP app.py
ENV CAM_UPLOADS_DIR /home/ubuntu/uploads
ENV CAM_TEMPLATES_DIR /home/ubuntu/templates/templates
ENV CAM_FONTS_DIR /home/ubuntu/templates/fonts
ENTRYPOINT ["gunicorn", "-b", ":9001", "--access-logfile", "-", "--error-logfile", "-", "app:app"]