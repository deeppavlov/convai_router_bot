from setuptools import setup, find_packages

setup(
    name="routerbot_model",
    version="0.0.1",
    packages=find_packages(include=['model', 'model.*']),
    install_requires=["mongoengine==0.15.0", "Pillow==6.1.0"],
)
