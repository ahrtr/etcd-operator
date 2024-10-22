ARG ARCH=amd64
FROM --platform=linux/${ARCH} gcr.io/distroless/static-debian12@sha256:41972110a1c1a5c0b6adb283e8aa092c43c31f7c5d79b8656fbffff2c3e61f05

ADD etcd-operator /usr/local/bin/

# Define default command.
CMD ["/usr/local/bin/etcd-operator"]
