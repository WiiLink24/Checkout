FROM mcr.microsoft.com/playwright/python:v1.58.0-noble

WORKDIR /home/ubuntu

# Copy requirements first as to not disturb cache for other changes.
COPY requirements.txt .

RUN pip3 install -r requirements.txt && \
  pip3 install gunicorn

RUN playwright install

USER ubuntu

# Finally, copy the entire source.
COPY . .

ENV FLASK_APP app.py
ENTRYPOINT ["gunicorn", "-b", ":9001", "--access-logfile", "-", "--error-logfile", "-", "app:app"]
