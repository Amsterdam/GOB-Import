FROM amsterdam/docker_python:latest
MAINTAINER datapunt@amsterdam.nl

# Install gobimportclient in /app folder
WORKDIR /app

# Install required Python packages
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy gobimportclient module
COPY gobimportclient gobimportclient

# For the current test period, add the datafiles
COPY example example

CMD ["python", "-m", "gobimportclient"]
