FROM tensorflow/tensorflow

Run pip install kafka-python
COPY ./consumer.py ./consumer.py
COPY ./ip_config.txt ./ip_config.txt
COPY ./port.txt ./port.txt
COPY ./partition_number.txt /partition_number.txt
COPY ./micro_batch_size.txt ./micro_batch_size.txt
COPY ./model_path.txt ./model_path.txt

CMD ["./consumer.py"]
ENTRYPOINT ["python"]
