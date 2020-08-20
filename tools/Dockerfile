FROM infsec/verimon

RUN sudo apt-get update && sudo apt-get install --no-install-recommends -y \
    openjdk-8-jdk \
    && sudo rm -rf /var/lib/apt/lists/*

USER root
ENV SDIR /
ENV WDIR /work
RUN mkdir -p ${SDIR}
RUN mkdir -p ${WDIR}
WORKDIR ${WDIR}
ADD . ${SDIR}
RUN chmod +x ${SDIR}/run.sh
ENTRYPOINT ["/run.sh"]

