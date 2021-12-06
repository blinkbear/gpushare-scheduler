FROM python:3.6
RUN pip install kubernetes
RUN pip install numpy
COPY admin.conf /admin.conf
COPY ./*.py /
CMD python /schedule_policy.py