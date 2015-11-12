from setuptools import setup

setup(
    name="frankenflow",
    version="0.1",
    py_modules=["frankenflow"],
    install_requires=[
        "paramiko",
        "celery",
        "networkx",
        "flask",
        "requests"
    ]
)
