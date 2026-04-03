from setuptools import setup


setup(
    name="pynetbox-wrapper",
    version="0.1.0",
    description="Unofficial helper wrapper around pynetbox extending NetBox client functionality",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    author="Eric Chester",
    python_requires=">=3.12",
    py_modules=["pynetbox2"],
    install_requires=[
        "deepdiff>=8.0.1",
        "pynetbox>=7.3.3",
        "redis>=5.0.0",
        "netboxlabs-diode-sdk>=1.10.0",
    ],
    license="Apache-2.0",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
    ],
)
