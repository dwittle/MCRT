# setup.py
"""Setup script for the Media Consolidation Tool."""

from setuptools import setup, find_packages

setup(
    name="media-consolidation-tool",
    version="2.0.0",
    description="Enhanced media consolidation and review tool with checkpoint support",
    long_description=open("README.md").read() if os.path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    author="Media Tool Team",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "Pillow>=8.0.0",
        "imagehash>=4.0.0",
        "tqdm>=4.50.0",
    ],
    entry_points={
        "console_scripts": [
            "media-tool=media_tool.main:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: End Users/Desktop",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Multimedia :: Graphics",
        "Topic :: System :: Archiving",
    ],
)

# requirements.txt
# Core dependencies
Pillow>=8.0.0
imagehash>=4.0.0
tqdm>=4.50.0

# Development dependencies (optional)
pytest>=6.0.0
pytest-cov>=2.10.0
black>=21.0.0
flake8>=3.8.0
mypy>=0.800