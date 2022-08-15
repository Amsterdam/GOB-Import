FROM amsterdam/python:3.9-buster as application
MAINTAINER datapunt@amsterdam.nl

# Install gobimport in /app folder
WORKDIR /app

# Copy the required db drivers
COPY drivers drivers

# Install Oracle driver
# https://oracle.github.io/odpi/doc/installation.html#linux
ENV ORACLE_DIR=/app/opt/oracle
RUN mkdir -p ${ORACLE_DIR}
RUN unzip drivers/instantclient-basiclite-linux.x64-21.4.0.0.0dbru.zip -d ${ORACLE_DIR}
ENV LD_LIBRARY_PATH=${ORACLE_DIR}/instantclient_21_4:$LD_LIBRARY_PATH
RUN apt-get update && apt-get -y --no-install-recommends install libaio1 unixodbc-dev apt-transport-https

# Install SQL Server driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Install GDAL
RUN apt-get install -y libgdal-dev

# Update C env vars so compiler can find gdal
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Install gdal separately from the rest of the python requirements.
# This avoids that changes to requirements.txt incure a time consuming gdal build
RUN LIBGDAL_VERSION=$(gdal-config --version) && pip3 install GDAL==${LIBGDAL_VERSION}
# Install required Python packages
COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip3 install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

# Copy gobimport module
COPY gobimport gobimport

CMD ["python", "-m", "gobimport"]
FROM application as development
# Install GOB-Core and GOB-Config in development mode. Will be overridden with
# mounts from the developer filesystem. Changes made to GOB-Core and GOB-Config
# will be available immediately.
RUN git clone --depth=1 https://github.com/Amsterdam/GOB-Core.git
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install -e /app/GOB-Core
RUN git clone --depth=1 https://github.com/Amsterdam/GOB-Config.git
RUN pip install -e /app/GOB-Config
COPY gobimport gobimport

FROM application as test
# Remove gobcore/gobconfig tests
RUN rm -rf /app/src/gobcore/tests
RUN rm -rf /app/src/gobconfig/tests

# Copy tests
COPY tests tests
COPY .flake8 .flake8
COPY test.sh test.sh

# Copy jenkins files
COPY .jenkins /.jenkins

RUN mkdir -p /app/shared/message_broker
