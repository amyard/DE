# https://github.com/apache/superset/blob/master/docker-compose.yml
FROM apache/superset:latest

USER root

# add extra packages here to connect to different DB - https://superset.apache.org/docs/configuration/databases/
# RUN pip install mysqlclient

ENV ADMIN_USERNAME $ADMIN_USERNAME
ENV ADMIN_EMAIL $ADMIN_EMAIL
ENV ADMIN_PASSWORD $ADMIN_PASSWORD

COPY ./superset-init.sh /superset-init.sh
COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH /app/superset_config.py

USER superset
ENTRYPOINT ["/superset-init.sh"]
