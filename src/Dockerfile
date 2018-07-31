FROM amsterdam/docker_python:latest
MAINTAINER datapunt@amsterdam.nl

# Install client in /app folder
WORKDIR /app

# Install required Python packages
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy client module
COPY client client

# Copy tests
COPY test.sh .flake8 ./

# Copy jenkins files
COPY .jenkins /.jenkins
