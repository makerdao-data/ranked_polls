FROM python:3.8

RUN mkdir /app
WORKDIR /app
COPY . /app/

RUN python3 -m venv /env
RUN . /env/bin/activate
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

CMD streamlit run src/main.py