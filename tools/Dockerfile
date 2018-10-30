FROM ubuntu:18.04

RUN apt-get update && apt-get install --no-install-recommends -y \ 
    maven \
    && rm -rf /var/lib/apt/lists/*

USER root
ENV WDIR /home/ubuntu
RUN mkdir -p ${WDIR}
WORKDIR ${WDIR}
ADD . ${WDIR}
RUN chmod +x ${WDIR}/run.sh
RUN mvn package
ENTRYPOINT ["/home/ubuntu/run.sh"]

