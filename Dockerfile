FROM ubuntu:trusty

LABEL maintainer="arjan.verkerk@nelen-schuurmans.nl"

# Get rid of debconf messages like "unable to initialize frontend: Dialog".
# https://github.com/docker/docker/issues/4032#issuecomment-192327844
ARG DEBIAN_FRONTEND=noninteractive

# Note: The official Debian and Ubuntu images automatically run apt-get clean,
# so explicit invocation is not required. See RUN apt-get in "Best practices
# for writing Dockerfiles". https://docs.docker.com/engine/userguide/↵
# eng-image/dockerfile_best-practices/
RUN apt-get update && apt-get install -y \
    git \
    locales \
    python3-pip \
    python3-dev \
    python3-grib \
    python3-gdal \
    libnetcdf-dev \
    libhdf5-serial-dev \
    && rm -rf /var/lib/apt/lists/*

RUN locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8

RUN pip3 install --upgrade pip virtualenv

# Trick buildout sysegg into thinking pygdal is available
RUN ln -s \
    /usr/lib/python3/dist-packages/GDAL-1.10.1.egg-info \
    /usr/lib/python3/dist-packages/pygdal-1.10.1.egg-info

# Create a nens user and group, with IDs matching those of the developer.
# The default values can be overridden at build-time via:
#
# docker-compose build --build-arg uid=`id -u` --build-arg gid=`id -g` lib
#
# The -l option is to fix a problem with large user IDs (e.g. 1235227245).
# https://forums.docker.com/t/run-adduser-seems-to-hang-with-large-uid/27371/3
# https://github.com/moby/moby/issues/5419
ARG uid=1000
ARG gid=1000
RUN groupadd -g $gid nens && useradd -lm -u $uid -g $gid nens

VOLUME /code
WORKDIR /code
USER nens
