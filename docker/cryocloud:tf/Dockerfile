# We're using Python3 for CryoCloud
FROM tensorflow/tensorflow:latest-devel-py3

WORKDIR /cc

# Copy CryoCore & CryoCloud 
ADD . /cc
ADD ksat/.config /root/.cryoconfig
ADD docker/cryocloud/requirements.txt /cc

# GDAL
# ADD docker/cryocloud/gdal-docker-master/ /usr/local/src/gdal-docker
#RUN /usr/local/src/gdal-docker/build.sh


# RUN apt-get update && apt-get install psutil

# Raster IO++
RUN add-apt-repository ppa:ubuntugis/ppa
RUN apt-get -y update
RUN apt-get -y install python-numpy gdal-bin libgdal-dev

RUN pip3 install rasterio

RUN pip3 install -r requirements.txt

# Trim away cache files - no need for these
RUN apt-get clean

ENV NAME CryoCloud
ENV PYTHONPATH="${PYTHONPATH}:/cc/:/cclib"
ENV PATH="${PATH}:/cc/bin"

RUN rm /usr/bin/python
RUN ln -s python3.5 /usr/bin/python

RUN mkdir /cclib

# Do we have an application to run?
# We're starting a CryoCloud node for now
# CMD ["python3", "./CryoCloud/Tools/node.py"]

# RUN PYTHONPATH=. python3 ./CryoCore/Tools/TailLog.py


# We test this by printing logs (should list something)
# CMD ["python3", "./CryoCloud/Tools/node.py"]
# CMD ["python3", "./CryoCore/Tools/TailLog.py"]