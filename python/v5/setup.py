from setuptools import find_packages, setup

setup(
    name='rocketmq-python-client',
    version='5.0.5',
    packages=find_packages(),
    install_requires=[
        "grpcio>=1.5.0",
        "grpcio-tools>=1.5.0",
        'protobuf',
        "opentelemetry-api>=1.2.0",
        "opentelemetry-sdk>=1.2.0",
        "opentelemetry-exporter-otlp>=1.2.0"
    ],
    python_requires='>=3.7',
)