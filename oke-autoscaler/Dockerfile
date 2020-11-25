FROM oraclelinux:7-slim
ENV OCI_CLI_SUPPRESS_FILE_PERMISSIONS_WARNING=True
ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/rh/rh-python36/root/usr/bin:/opt/rh/rh-python36/root/usr/bin/oci:${PATH}"
ENV LC_ALL=en_US.utf8
ENV LANG=en_US.utf8
ENV KUBECONFIG=/tmp/kubeconfig
ARG CLI_VERSION=2.10.3
RUN mkdir /oci-cli
RUN mkdir /function
ADD requirements.txt /function/
WORKDIR /function
RUN yum -y install oracle-release-el7 && \
    yum -y install oracle-softwarecollection-release-el7 && \
    yum-config-manager --enable software_collections && \
    yum-config-manager --enable ol7_latest ol7_optional_latest ol7_addons && \
    yum-config-manager --disable ol7_ociyum_config && \
    yum -y install scl-utils && \
    yum -y install rh-python36 && \
    yum -y install gcc && \
    yum -y install wget && \
    yum -y install unzip && \
    yum -y install jq && \
    yum -y install kubectl && \
    export PATH=$PATH:/opt/rh/rh-python36/root/usr/bin && \
    rm -rf /var/cache/yum && \
    pip3 install --no-cache --no-cache-dir -r requirements.txt && rm -fr ~/.cache/pip /tmp* requirements.txt func.yaml Dockerfile .venv
WORKDIR /oci-cli
RUN wget -qO- -O oci-cli.zip "https://github.com/oracle/oci-cli/releases/download/v${CLI_VERSION}/oci-cli-${CLI_VERSION}.zip" && \
    unzip -q oci-cli.zip -d .. && \
    rm oci-cli.zip && \
    pip3 install oci_cli-*-py2.py3-none-any.whl && \
    yes | oci setup autocomplete && \
    groupadd --gid 1000 fn && \
    adduser --uid 1000 --gid fn fn
ADD . /function/
ENTRYPOINT ["/opt/rh/rh-python36/root/usr/bin/fdk", "/function/func.py", "handler"]