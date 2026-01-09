from setuptools import setup, find_packages
setup(
    name="vnai",
    version="2.2.4",
    packages=find_packages(where="."),
    install_requires=[
"requests>=2.31.0",
"psutil>=5.8.0",
    ],
    extras_require={
"dev": ["pytest>=7.0.0"],
    },
    python_requires=">=3.10",
    description="Analytics and optimization library for vnstock ecosystem",
    long_description=open("README.md").read() if __name__ =='__main__' else"",
    long_description_content_type="text/markdown",
    author="Vnstock Team",
    author_email="support@vnstocks.com",
    classifiers=[
"Programming Language :: Python :: 3",
"Programming Language :: Python :: 3.10",
"Programming Language :: Python :: 3.11",
"Programming Language :: Python :: 3.12",
"Programming Language :: Python :: 3.13",
    ],
)