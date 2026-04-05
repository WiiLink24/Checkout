FROM python:3.14

RUN addgroup --gid 1000 server && adduser --uid 1000 --gid 1000 --system server
WORKDIR /home/server

# Copy requirements first as to not disturb cache for other changes.
COPY requirements.txt .

RUN pip3 install -r requirements.txt && \
  pip3 install gunicorn

USER server

# Finally, copy the entire source.
COPY . .

ENV FLASK_APP app.py
ENTRYPOINT ["gunicorn", "-b", ":9001", "--access-logfile", "-", "--error-logfile", "-", "app:app"]
