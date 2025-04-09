"""
Setup script for the opendatasoft-mcp-server package.
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="opendatasoft-mcp-server",
    version="0.1.0",
    description="An MCP server for interacting with the Opendatasoft API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Claude Developer",
    author_email="example@example.com",
    url="https://github.com/your-username/opendatasoft-mcp-server",
    packages=find_packages(),
    install_requires=[
        "httpx>=0.25.0",
        "mcp>=1.2.0",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
            "opendatasoft-mcp=src.main:main",
        ],
    },
)