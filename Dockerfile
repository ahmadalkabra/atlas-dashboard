FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY fetch_flyover.py fetch_powpeg.py fetch_btc_locked.py ./
COPY generate_report.py ./
COPY fetch_loop.sh ./
RUN chmod +x fetch_loop.sh

RUN mkdir -p data pages/data

CMD ["./fetch_loop.sh"]
