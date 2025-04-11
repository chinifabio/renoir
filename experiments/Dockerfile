FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV SSH_USERNAME="ubuntu"
ENV SSHD_CONFIG_ADDITIONAL=""

RUN apt-get update \
    && apt-get install -y iproute2 iputils-ping openssh-server telnet iperf3 iptables hyperfine python3 python3-pydantic python3-yaml python-is-python3 ansible-core --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && mkdir -p /run/sshd \
    && chmod 755 /run/sshd \
    && if ! id -u "$SSH_USERNAME" > /dev/null 2>&1; then useradd -ms /bin/bash "$SSH_USERNAME"; fi \
    && chown -R "$SSH_USERNAME":"$SSH_USERNAME" /home/"$SSH_USERNAME" \
    && chmod 755 /home/"$SSH_USERNAME" \
    && mkdir -p /home/"$SSH_USERNAME"/.ssh \
    && chown "$SSH_USERNAME":"$SSH_USERNAME" /home/"$SSH_USERNAME"/.ssh \
    && echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config \
    && echo "PermitRootLogin no" >> /etc/ssh/sshd_config

RUN ansible-galaxy collection install ansible.posix

COPY start.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/start.sh

COPY shape-network.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/shape-network.sh

EXPOSE 22

WORKDIR /renoir

CMD ["/usr/local/bin/start.sh"]