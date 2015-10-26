FROM centos:7
MAINTAINER ben@perimeterx.com

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN yum -y group mark convert
RUN yum -y groupinstall "Development Tools"
RUN yum -y install git libasan snappy-devel boost-devel rpm-build wget unzip socat
RUN wget "http://downloads.sourceforge.net/project/scons/scons/2.3.6/scons-2.3.6-1.noarch.rpm?r=http%3A%2F%2Fsourceforge.net%2Fprojects%2Fscons%2Ffiles%2Fscons%2F2.3.6%2F&ts=1439720375&use_mirror=skylineservers" -O scons.rpm && \
    rpm -i scons.rpm

RUN wget http://googletest.googlecode.com/files/gtest-1.7.0.zip && \
          unzip gtest-1.7.0.zip && \
          cd gtest-1.7.0 && \
          ./configure && \
          make && \
          cd lib/.libs && \
          mkdir -p /usr/lib64 && \
          mv *.a *.lai *.so* ../*.la /usr/lib64 && \
          ldconfig && \
          cd ../.. && \
          cp -a include/gtest /usr/include

RUN cd /root && \
    git clone https://github.com/ifwe/bruce.git && \
    cd bruce && \
    cd src/bruce && \
    scons -Q --up --release bruce && \
    mkdir -p /opt/bruce/bin/ && \
    cp /root/bruce/out/release/bruce/bruce /opt/bruce/bin/


RUN wget http://peak.telecommunity.com/dist/ez_setup.py
RUN python ez_setup.py
RUN easy_install pip
ADD datadogsync.py /root/
RUN pip install datadog

RUN mkdir -p /etc/bruce
ADD bruce_conf.xml /etc/bruce/
ADD start.sh /etc/bruce/

EXPOSE 9090

CMD sh /etc/bruce/start.sh