FROM tensorflow/tensorflow

Run pip install kafka-python
COPY ./consumer.py ./consumer.py
COPY ./ip_config.txt ./ip_config.txt
COPY ./port.txt ./port.txt

CMD ["./consumer.py"]
ENTRYPOINT ["python"]
