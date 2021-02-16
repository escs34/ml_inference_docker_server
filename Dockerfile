FROM tensorflow/tensorflow

Run pip install kafka-python
COPY ./consumer.py ./consumer.py
COPY ./ip_config.txt ./ip_config.txt
COPY ./port.txt ./port.txt
COPY ./partition_number.txt /partition_number.txt
COPY ./micro_batch_size.txt ./micro_batch_size.txt
COPY ./model_path.txt ./model_path.txt
Run mkdir models
Run mkdir models/cifar10_base_models

COPY ./models/cifar10_base_models/mobilenet_v1.h5 ./models/cifar10_base_models/mobilenet_v1.h5

CMD ["./consumer.py"]
ENTRYPOINT ["python"]
